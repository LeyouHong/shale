package cache

import (
	"fmt"
	"sync"
	"testing"
)

func block(n int) []byte { return make([]byte, n) }

func TestPutGet(t *testing.T) {
	c := New(1024)

	k := Key{FileNum: 1, Offset: 0}
	if _, ok := c.Get(k); ok {
		t.Error("空缓存不该命中")
	}

	c.Put(k, []byte("hello"))
	got, ok := c.Get(k)
	if !ok {
		t.Fatal("刚放进去的应该能取到")
	}
	if string(got) != "hello" {
		t.Errorf("值 = %q，期望 hello", got)
	}

	hits, misses := c.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("命中 %d 次、未命中 %d 次，期望 1 和 1", hits, misses)
	}
}

func TestKeyDistinguishesFileAndOffset(t *testing.T) {
	c := New(1024)
	c.Put(Key{FileNum: 1, Offset: 0}, []byte("a"))
	c.Put(Key{FileNum: 1, Offset: 100}, []byte("b"))
	c.Put(Key{FileNum: 2, Offset: 0}, []byte("c"))

	cases := []struct {
		k    Key
		want string
	}{
		{Key{1, 0}, "a"},
		{Key{1, 100}, "b"},
		{Key{2, 0}, "c"},
	}
	for _, tc := range cases {
		got, ok := c.Get(tc.k)
		if !ok || string(got) != tc.want {
			t.Errorf("%+v = (%q,%v)，期望 %q", tc.k, got, ok, tc.want)
		}
	}
}

// TestEviction 验证超容时从最久未用的开始淘汰。
func TestEviction(t *testing.T) {
	c := New(300) // 只装得下 3 个 100 字节的块

	for i := 0; i < 3; i++ {
		c.Put(Key{FileNum: 1, Offset: uint64(i)}, block(100))
	}
	if c.Len() != 3 {
		t.Fatalf("应该有 3 个块，实际 %d 个", c.Len())
	}

	// 访问 0 号，让它变成"最近使用"
	c.Get(Key{1, 0})

	// 再放一个，必须淘汰掉最久没用的（1 号）
	c.Put(Key{1, 3}, block(100))

	if _, ok := c.Get(Key{1, 1}); ok {
		t.Error("1 号是最久未用的，应该被淘汰")
	}
	if _, ok := c.Get(Key{1, 0}); !ok {
		t.Error("0 号刚被访问过，不该被淘汰")
	}
	if _, ok := c.Get(Key{1, 3}); !ok {
		t.Error("3 号是刚放进去的")
	}
	if c.Used() > 300 {
		t.Errorf("占用 %d 字节，超过容量 300", c.Used())
	}
}

func TestOversizedBlockRejected(t *testing.T) {
	c := New(100)
	c.Put(Key{1, 0}, block(200)) // 比整个缓存还大
	if c.Len() != 0 {
		t.Error("超大的块不该被放进去")
	}
	if c.Used() != 0 {
		t.Errorf("占用应为 0，实际 %d", c.Used())
	}
}

func TestUpdateExisting(t *testing.T) {
	c := New(1024)
	k := Key{1, 0}
	c.Put(k, block(100))
	used1 := c.Used()

	c.Put(k, block(200)) // 同一个 key 换更大的内容
	if c.Len() != 1 {
		t.Errorf("应该还是 1 个块，实际 %d 个", c.Len())
	}
	if c.Used() != used1+100 {
		t.Errorf("占用 = %d，期望 %d", c.Used(), used1+100)
	}
}

// TestEvictFile 验证能清掉某个文件的全部缓存 ——
// compaction 删文件后必须这么做，否则文件号被复用时会读到旧数据。
func TestEvictFile(t *testing.T) {
	c := New(10000)
	for i := 0; i < 5; i++ {
		c.Put(Key{FileNum: 1, Offset: uint64(i)}, block(100))
		c.Put(Key{FileNum: 2, Offset: uint64(i)}, block(100))
	}
	if c.Len() != 10 {
		t.Fatalf("应有 10 个块，实际 %d 个", c.Len())
	}

	c.EvictFile(1)

	if c.Len() != 5 {
		t.Errorf("清掉文件 1 之后应剩 5 个块，实际 %d 个", c.Len())
	}
	for i := 0; i < 5; i++ {
		if _, ok := c.Get(Key{1, uint64(i)}); ok {
			t.Errorf("文件 1 的块 %d 应该被清掉了", i)
		}
		if _, ok := c.Get(Key{2, uint64(i)}); !ok {
			t.Errorf("文件 2 的块 %d 不该受影响", i)
		}
	}
	if c.Used() != 500 {
		t.Errorf("占用 = %d，期望 500", c.Used())
	}
}

func TestDisabledCache(t *testing.T) {
	c := New(0) // 容量 0 = 禁用
	c.Put(Key{1, 0}, []byte("x"))
	if _, ok := c.Get(Key{1, 0}); ok {
		t.Error("禁用的缓存不该命中")
	}
	if c.Len() != 0 {
		t.Error("禁用的缓存不该存东西")
	}
}

func TestNilCacheIsSafe(t *testing.T) {
	var c *Cache
	// 所有方法对 nil 接收者都要安全 —— 让调用方不必到处判空
	c.Put(Key{1, 0}, []byte("x"))
	if _, ok := c.Get(Key{1, 0}); ok {
		t.Error("nil 缓存不该命中")
	}
	c.EvictFile(1)
	if c.Len() != 0 || c.Used() != 0 || c.HitRate() != 0 {
		t.Error("nil 缓存的统计应该都是 0")
	}
}

func TestHitRate(t *testing.T) {
	c := New(1024)
	c.Put(Key{1, 0}, []byte("x"))
	for i := 0; i < 7; i++ {
		c.Get(Key{1, 0}) // 命中
	}
	for i := 0; i < 3; i++ {
		c.Get(Key{1, uint64(i + 100)}) // 未命中
	}
	if rate := c.HitRate(); rate < 0.69 || rate > 0.71 {
		t.Errorf("命中率 = %.2f，期望约 0.70", rate)
	}
}

// TestConcurrent 验证并发安全（配合 -race 跑）。
func TestConcurrent(t *testing.T) {
	c := New(100 << 10)
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := Key{FileNum: uint64(id % 3), Offset: uint64(i % 50)}
				if _, ok := c.Get(k); !ok {
					c.Put(k, block(100))
				}
				if i%500 == 0 {
					c.EvictFile(uint64(id % 3))
				}
			}
		}(g)
	}
	wg.Wait()

	if c.Used() > 100<<10 {
		t.Errorf("并发之后占用 %d 超过了容量", c.Used())
	}
}

func BenchmarkGetHit(b *testing.B) {
	c := New(10 << 20)
	for i := 0; i < 1000; i++ {
		c.Put(Key{1, uint64(i)}, block(4096))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(Key{1, uint64(i % 1000)})
	}
}

func BenchmarkPutWithEviction(b *testing.B) {
	c := New(1 << 20) // 小容量，持续触发淘汰
	blk := block(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(Key{1, uint64(i)}, blk)
	}
}

func ExampleCache() {
	c := New(1 << 20)
	k := Key{FileNum: 7, Offset: 4096}

	if _, ok := c.Get(k); !ok {
		c.Put(k, []byte("从磁盘读来的块"))
	}
	got, _ := c.Get(k)
	fmt.Println(string(got))
	// Output:
	// 从磁盘读来的块
}
