// Package skiplist 实现跳表 —— MemTable 的底层数据结构。
//
// 对应 LSM 原理的：第 4 步（在内存里排序）
//
// # 为什么是跳表
//
// MemTable 需要一个「有序 + 查找快 + 插入快」的内存结构。
// 从最朴素的有序链表出发就能推出跳表：
//
//	层0    1 ──→ 3 ──→ 5 ──→ 7 ──→ 9 ──→ 11 ──→ 13 ──→ 15
//
// 链表有序，但查找只能一个个往后爬 —— 而且【没法二分】，
// 因为二分需要"直接跳到中间那个"，链表只能顺着指针走。
//
// 那就挑一部分节点，在上面再串一条链当"快速通道"：
//
//	层1    1 ─────────→ 5 ─────────→ 9 ─────────→ 13
//	层0    1 ──→ 3 ──→ 5 ──→ 7 ──→ 9 ──→ 11 ──→ 13 ──→ 15
//
// 一层能快一倍，多叠几层就更快。这就是跳表：
//
//	层3    1 ──────────────────────────────────→ 15
//	层2    1 ──────────→ 7 ──────────────────────→ 15
//	层1    1 ─────→ 5 ─→ 7 ─────────→ 11 ─────────→ 15
//	层0    1 ─→ 3 ─→ 5 ─→ 7 ─→ 9 ─→ 11 ─→ 13 ─→ 15
//
// 查找规则只有一句：**能往右就往右，右边太大就下降一层**（像下楼梯）。
// 每上一层节点数减少到 1/4，所以查找是 O(log n)。
//
// # 为什么不用平衡树
//
//	· 插入只需要改几个指针，不像平衡树要旋转、调整整棵树
//	· 因此并发实现简单得多（本项目 M9 之前先用外部互斥）
//	· 代码量小一个数量级，适合读懂
//
// # 并发
//
// 本实现【不是】并发安全的，调用方必须自己保证互斥。
// DB 层目前用一把大锁保护，M9 再考虑无锁化。
package skiplist

import (
	"math/rand"
)

const (
	// maxHeight 是跳表的最大层数。
	// 每层过滤到 1/4，12 层能有效索引 4^12 ≈ 1600 万个元素，
	// 对一个几十 MB 的 MemTable 绰绰有余。
	maxHeight = 12

	// branching 决定每层保留多少节点：1/branching。
	//
	// 取 4 而不是 2 —— 这是 LevelDB 的选择。
	// 2 的话每个节点平均 2 层，4 的话平均 1.33 层，
	// 指针数组更短、内存省 1/3，而查找步数只多一点点。
	branching = 4

	// 随机数种子固定，让同样的插入序列总是构造出同样形状的跳表。
	// 测试因此可复现；跳表的正确性不依赖种子的随机质量。
	defaultSeed = 0xdeadbeef
)

// Comparator 定义 key 的排序规则，语义同 bytes.Compare。
//
// 之所以做成参数而不是写死，是因为跳表本身是通用结构：
// MemTable 会传入 ikey.Compare（user key 升序 + seq 降序），
// 而单元测试可以直接传 bytes.Compare 来独立验证跳表逻辑。
type Comparator func(a, b []byte) int

type node struct {
	key   []byte
	value []byte

	// next[i] 是本节点在第 i 层的后继。
	// len(next) 就是这个节点的高度 —— 高度越高，越可能被上层的"快速通道"用到。
	next []*node
}

// SkipList 是一个有序的 key-value 集合。
//
// 允许存在相同的 key（本项目里不会发生：MemTable 存的是内部 key，
// 每条记录的 seq 都不同，天然唯一）。
type SkipList struct {
	// head 是哨兵节点，不存数据，只提供每一层的起点。
	head *node

	// height 是当前实际用到的最大层数（1 ~ maxHeight）。
	// 一开始是 1，随着插入慢慢长高。
	height int

	cmp Comparator
	rnd *rand.Rand

	count int
	size  int64
}

// New 创建一个空跳表。
func New(cmp Comparator) *SkipList {
	return NewWithSeed(cmp, defaultSeed)
}

// NewWithSeed 用指定随机种子创建跳表，便于测试不同的层高分布。
func NewWithSeed(cmp Comparator, seed int64) *SkipList {
	return &SkipList{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		cmp:    cmp,
		rnd:    rand.New(rand.NewSource(seed)),
	}
}

// Len 返回元素个数。
func (s *SkipList) Len() int { return s.count }

// Size 返回估算的内存占用（字节）。
//
// 只是估算：算了 key、value 的实际长度，加上每个节点的固定开销。
// MemTable 用它判断"是不是该刷盘了"，不需要精确。
func (s *SkipList) Size() int64 { return s.size }

// Height 返回当前的层数，主要用于测试和观察。
func (s *SkipList) Height() int { return s.height }

// Insert 插入一对 key-value。
//
// key 和 value 都会被【复制】—— 调用方可以放心复用自己的缓冲区。
func (s *SkipList) Insert(key, value []byte) {
	var prev [maxHeight]*node
	s.findGreaterOrEqual(key, prev[:])

	h := s.randomHeight()
	if h > s.height {
		// 新节点比当前跳表还高：超出的那几层此前是空的，
		// 它们的前驱只能是 head。
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	n := &node{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		next:  make([]*node, h),
	}

	// 逐层接线：把新节点插到 prev[i] 和 prev[i].next[i] 之间。
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}

	s.count++
	// 节点固定开销的粗略估计：切片头 + 指针数组 + 对象头
	s.size += int64(len(key)+len(value)) + int64(h*8) + 64
}

// Get 精确查找一个 key。
func (s *SkipList) Get(key []byte) ([]byte, bool) {
	n := s.findGreaterOrEqual(key, nil)
	if n != nil && s.cmp(n.key, key) == 0 {
		return n.value, true
	}
	return nil, false
}

// Contains 判断 key 是否存在。
func (s *SkipList) Contains(key []byte) bool {
	_, ok := s.Get(key)
	return ok
}

// findGreaterOrEqual 返回第一个 >= key 的节点，没有则返回 nil。
//
// 如果 prev 非 nil，会把每一层上"最后一个 < key 的节点"记录进去 ——
// 插入时需要这些前驱来接线。
//
// 这个函数就是包注释里那句「能往右就往右，右边太大就下降一层」的代码化。
func (s *SkipList) findGreaterOrEqual(key []byte, prev []*node) *node {
	x := s.head
	level := s.height - 1

	for {
		next := x.next[level]
		if next != nil && s.cmp(next.key, key) < 0 {
			x = next // 右边还比目标小 —— 继续往右
			continue
		}
		// 右边到头了或者太大了 —— 记下前驱，往下一层
		if prev != nil {
			prev[level] = x
		}
		if level == 0 {
			return next // 到底了，next 就是第一个 >= key 的
		}
		level--
	}
}

// findLessThan 返回最后一个 < key 的节点。
// 没有比它小的元素时返回 head（哨兵），由调用方判断。
func (s *SkipList) findLessThan(key []byte) *node {
	x := s.head
	level := s.height - 1
	for {
		next := x.next[level]
		if next != nil && s.cmp(next.key, key) < 0 {
			x = next
			continue
		}
		if level == 0 {
			return x
		}
		level--
	}
}

// findLast 返回最后一个节点，跳表为空时返回 head。
func (s *SkipList) findLast() *node {
	x := s.head
	level := s.height - 1
	for {
		next := x.next[level]
		if next != nil {
			x = next
			continue
		}
		if level == 0 {
			return x
		}
		level--
	}
}

// randomHeight 用"抛硬币"决定新节点的高度。
//
//	一定进第 0 层
//	之后每次有 1/branching 的概率再升一层，直到失败或触顶
//
// 结果：约 1/4 的节点在第 1 层，1/16 在第 2 层……
// 天然形成"越往上越稀疏"的形状。
//
// 为什么不老老实实"每隔 4 个提一个"？因为那样每插入一个节点，
// 后面所有节点的层级都要重排。随机化的期望性能同样是 O(log n)，
// 但【插入只影响局部】。
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && s.rnd.Intn(branching) == 0 {
		h++
	}
	return h
}

// ── 迭代器 ──────────────────────────────────────────────────

// Iterator 按 key 升序遍历跳表。
//
// 遍历期间不能修改跳表（本项目里 MemTable 冻结之后才会被遍历，天然满足）。
type Iterator struct {
	list *SkipList
	node *node
}

// NewIterator 创建一个迭代器。初始状态是无效的，需要先 Seek 或 SeekToFirst。
func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{list: s}
}

// Valid 返回当前是否停在有效位置上。
func (it *Iterator) Valid() bool { return it.node != nil }

// Key 返回当前 key。仅在 Valid 时有意义。
func (it *Iterator) Key() []byte { return it.node.key }

// Value 返回当前 value。仅在 Valid 时有意义。
func (it *Iterator) Value() []byte { return it.node.value }

// Seek 定位到第一个 >= key 的位置。
func (it *Iterator) Seek(key []byte) {
	it.node = it.list.findGreaterOrEqual(key, nil)
}

// SeekToFirst 定位到最小的 key。
func (it *Iterator) SeekToFirst() {
	it.node = it.list.head.next[0]
}

// SeekToLast 定位到最大的 key。
func (it *Iterator) SeekToLast() {
	n := it.list.findLast()
	if n == it.list.head {
		it.node = nil // 空表
		return
	}
	it.node = n
}

// Next 移动到下一个 key。
func (it *Iterator) Next() {
	it.node = it.node.next[0]
}

// Prev 移动到上一个 key。
//
// 跳表没有反向指针，所以这里是"从头再找一次最后一个 < 当前 key 的节点"。
// 因此 Prev 比 Next 贵得多 —— 反向遍历整个跳表是 O(n log n) 而不是 O(n)。
// 好在实际用不到大量反向遍历。
func (it *Iterator) Prev() {
	n := it.list.findLessThan(it.node.key)
	if n == it.list.head {
		it.node = nil // 已经是第一个了
		return
	}
	it.node = n
}
