// Package cache 实现 Block 缓存 —— 一个容量受限的 LRU。
//
// # 它解决什么问题
//
// SSTable 的读取单位是块（4KB）。同一个块往往被反复读到：
// 热点 key 集中在少数几个块里、范围扫描会连续访问相邻块、
// 每个文件的 Index Block 更是每次查询都要碰。
//
// 每次都真读磁盘太浪费，所以把解压后的块缓存起来。
// 这相当于 InnoDB 的 Buffer Pool，是抵消 LSM 读放大的主要武器之一
// （另一个是布隆过滤器 —— 两者分工不同：
// 过滤器负责"根本不去读"，缓存负责"读过的不再读第二遍"）。
//
// # 为什么是 LRU
//
// 数据访问天然有局部性：刚被读过的块，很可能马上又被读。
// LRU 用"最近最少使用"近似"未来最不可能用"，简单且效果好。
//
// 本实现是并发安全的 —— SSTable 是只读的，多个 goroutine 会同时来查缓存。
package cache

import (
	"container/list"
	"sync"
)

// Key 唯一标识一个缓存项。
//
// 用「文件号 + 块偏移」而不是字符串拼接：结构体做 map key 没有分配开销，
// 而查缓存是极高频的操作。
type Key struct {
	FileNum uint64
	Offset  uint64
}

// entry 是链表节点里存的东西。
type entry struct {
	key   Key
	value []byte
}

// Cache 是一个容量受限的 LRU 缓存。零值不可用，请用 New 创建。
type Cache struct {
	mu       sync.Mutex
	capacity int64
	used     int64

	// lru 的表头是最近使用的，表尾是最久没用的。
	lru   *list.List
	items map[Key]*list.Element

	hits   int64
	misses int64
}

// New 创建一个容量为 capacity 字节的缓存。capacity <= 0 表示禁用缓存。
func New(capacity int64) *Cache {
	return &Cache{
		capacity: capacity,
		lru:      list.New(),
		items:    make(map[Key]*list.Element),
	}
}

// Get 查缓存。命中时返回的切片【不要修改】—— 它是共享的。
func (c *Cache) Get(key Key) ([]byte, bool) {
	if c == nil || c.capacity <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	// 命中：挪到表头，表示"最近用过"
	c.lru.MoveToFront(el)
	c.hits++
	return el.Value.(*entry).value, true
}

// Put 放入一个块。value 会被直接持有，调用方之后不要再修改它。
func (c *Cache) Put(key Key, value []byte) {
	if c == nil || c.capacity <= 0 {
		return
	}
	size := int64(len(value))
	if size > c.capacity {
		return // 单个块比整个缓存还大，放不进去
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		// 已经在里面了，更新内容并挪到表头
		old := el.Value.(*entry)
		c.used += size - int64(len(old.value))
		old.value = value
		c.lru.MoveToFront(el)
	} else {
		el := c.lru.PushFront(&entry{key: key, value: value})
		c.items[key] = el
		c.used += size
	}

	// 超容就从表尾开始淘汰 —— 那里是最久没被用过的
	for c.used > c.capacity && c.lru.Len() > 0 {
		c.evictOldest()
	}
}

func (c *Cache) evictOldest() {
	el := c.lru.Back()
	if el == nil {
		return
	}
	c.lru.Remove(el)
	e := el.Value.(*entry)
	delete(c.items, e.key)
	c.used -= int64(len(e.value))
}

// EvictFile 清掉某个文件的所有缓存块。
//
// compaction 删掉一个文件之后必须调用，否则那些块会一直占着容量，
// 而且文件号将来可能被复用，留着会读到错误的数据。
func (c *Cache) EvictFile(fileNum uint64) {
	if c == nil || c.capacity <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		e := el.Value.(*entry)
		if e.key.FileNum == fileNum {
			c.lru.Remove(el)
			delete(c.items, e.key)
			c.used -= int64(len(e.value))
		}
		el = next
	}
}

// Stats 返回命中次数和未命中次数。
func (c *Cache) Stats() (hits, misses int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}

// Used 返回已占用的字节数。
func (c *Cache) Used() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// Len 返回缓存的块数。
func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// HitRate 返回命中率，没有访问过时返回 0。
func (c *Cache) HitRate() float64 {
	hits, misses := c.Stats()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}
