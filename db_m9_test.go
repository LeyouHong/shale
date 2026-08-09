package shale

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 这个文件是 M9 的验收测试：后台执行 + 并发安全。

// TestImmutableQueueVisible 验证冻结之后、落盘之前的数据仍然读得到。
//
// 这是后台刷盘引入的新风险：MemTable 被换掉了，但数据还没进 SSTable，
// 查询必须记得去问那个"悬在中间"的 immutable。
func TestImmutableQueueVisible(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize: 8 << 10,
		MaxMemTables: 4, // 队列开大点，让 immutable 有机会堆着
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 2000
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// 不等后台，立刻全部读一遍 —— 此刻数据分散在
	// mem、imm 队列、以及已经刷完的 SSTable 里
	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("k%05d", i), fmt.Sprintf("v%d", i))
	}
	t.Logf("读取时队列里还有 %d 个 immutable 等待刷盘", db.Stats().ImmutableCount)
}

// TestIteratorSeesImmutables 验证迭代器也能看到 immutable 里的数据。
func TestIteratorSeesImmutables(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 8 << 10, MaxMemTables: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 1500
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}

	got := scanAll(t, db)
	if len(got) != n {
		t.Fatalf("扫出 %d 条，期望 %d 条 —— 可能漏掉了 immutable 里的数据", len(got), n)
	}
}

// TestWriteNotBlockedByFlush 验证前台写入不再等落盘。
//
// M8 之前 flush 是同步的：MemTable 一满，那次 Put 就得原地等着写完几 MB。
// 现在只是换个指针，落盘交给后台。
func TestWriteNotBlockedByFlush(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize: 64 << 10,
		MaxMemTables: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 200)
	var maxLatency time.Duration
	const n = 5000

	for i := 0; i < n; i++ {
		start := time.Now()
		if err := db.Put([]byte(fmt.Sprintf("k%06d", i)), val); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > maxLatency {
			maxLatency = d
		}
	}

	st := db.Stats()
	t.Logf("写入 %d 条：最大单次延迟 %v，flush %d 次、compaction %d 次、写入等待 %d 次",
		n, maxLatency, st.FlushCount, st.CompactionCount, st.WriteStalls)

	// 冻结只是换指针，正常情况下不该有几十毫秒的尖峰
	if maxLatency > 100*time.Millisecond {
		t.Errorf("最大单次写入延迟 %v 过高，前台似乎仍在等落盘", maxLatency)
	}
}

// TestWriteStallWhenBackgroundCantKeepUp 验证后台跟不上时会踩刹车。
//
// 刹车是必须的：不限制的话内存会无限增长，最终 OOM。
func TestWriteStallWhenBackgroundCantKeepUp(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        4 << 10, // 极小，逼它频繁冻结
		MaxMemTables:        2,       // 队列只有一个位置
		L0CompactionTrigger: 2,
		L0StopWritesTrigger: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 200)
	for i := 0; i < 8000; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%06d", i)), val); err != nil {
			t.Fatal(err)
		}
	}

	st := db.Stats()
	t.Logf("高压写入后：等待 %d 次、flush %d 次、compaction %d 次，L0 有 %d 个文件",
		st.WriteStalls, st.FlushCount, st.CompactionCount, st.Levels[0].NumFiles)

	// 队列上限被守住了
	if st.ImmutableCount >= db.opts.MaxMemTables {
		t.Errorf("immutable 队列有 %d 个，超过了上限 %d", st.ImmutableCount, db.opts.MaxMemTables)
	}
	// 数据一条不能少
	for i := 0; i < 8000; i += 137 {
		mustGet(t, db, fmt.Sprintf("k%06d", i), string(val))
	}
}

// TestCloseWaitsForBackground 验证 Close 会等后台干完，不会丢数据。
func TestCloseWaitsForBackground(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, &Options{MemTableSize: 8 << 10, MaxMemTables: 4})
	if err != nil {
		t.Fatal(err)
	}
	const n = 3000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}
	// 此刻后台很可能正在刷盘
	if err := db.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	// 重开之后数据必须齐全（没落盘的会从 WAL 恢复）
	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		mustGet(t, db2, fmt.Sprintf("k%05d", i), "v")
	}
}

// TestCrashWithPendingImmutables 验证 immutable 还没刷完就崩溃时不丢数据。
//
// 这依赖一条规则：immutable 对应的 WAL 在它落盘之前【不能删】。
func TestCrashWithPendingImmutables(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, &Options{MemTableSize: 8 << 10, MaxMemTables: 4})
	if err != nil {
		t.Fatal(err)
	}
	const n = 2000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
	}
	pending := db.Stats().ImmutableCount
	// 不 Close，直接崩溃

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()

	t.Logf("崩溃时队列里有 %d 个未落盘的 immutable，恢复了 %d 条记录",
		pending, db2.RecoveredEntries())
	for i := 0; i < n; i++ {
		mustGet(t, db2, fmt.Sprintf("k%05d", i), "v")
	}
}

// TestConcurrentReadWrite 是并发压测：多个 goroutine 同时读写。
func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 32 << 10, MaxMemTables: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		writers  = 4
		readers  = 4
		scanners = 2
		perGor   = 2000
	)

	var wg sync.WaitGroup
	var writeErrs, readErrs atomic.Int64

	// 每个 writer 负责一段独立的 key 空间，方便断言
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGor; i++ {
				key := fmt.Sprintf("w%d-k%06d", id, i)
				if err := db.Put([]byte(key), []byte("value")); err != nil {
					writeErrs.Add(1)
					return
				}
			}
		}(w)
	}

	// reader 读自己那段已经写过的部分
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < perGor; i++ {
				key := fmt.Sprintf("w%d-k%06d", rng.Intn(writers), rng.Intn(perGor))
				if _, err := db.Get([]byte(key)); err != nil && !errors.Is(err, ErrNotFound) {
					readErrs.Add(1)
					return
				}
			}
		}(r)
	}

	// scanner 不断做范围扫描
	for s := 0; s < scanners; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				it, err := db.NewIterator()
				if err != nil {
					readErrs.Add(1)
					return
				}
				n := 0
				for it.SeekToFirst(); it.Valid() && n < 500; it.Next() {
					n++
				}
				it.Close()
			}
		}()
	}

	wg.Wait()

	if writeErrs.Load() > 0 || readErrs.Load() > 0 {
		t.Fatalf("并发出错：写 %d 次、读 %d 次", writeErrs.Load(), readErrs.Load())
	}

	// 所有写入都必须在
	for w := 0; w < writers; w++ {
		for i := 0; i < perGor; i++ {
			mustGet(t, db, fmt.Sprintf("w%d-k%06d", w, i), "value")
		}
	}

	st := db.Stats()
	t.Logf("%d 写 + %d 读 + %d 扫描并发完成：%d 条记录，flush %d 次、compaction %d 次、等待 %d 次",
		writers, readers, scanners, writers*perGor,
		st.FlushCount, st.CompactionCount, st.WriteStalls)
}

// TestConcurrentBatchWrites 验证并发批量写的原子性。
func TestConcurrentBatchWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const gors, batches, perBatch = 8, 200, 10
	var wg sync.WaitGroup
	for g := 0; g < gors; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				batch := NewBatch()
				for j := 0; j < perBatch; j++ {
					batch.Put([]byte(fmt.Sprintf("g%d-b%03d-k%d", id, b, j)), []byte("v"))
				}
				if err := db.Write(batch); err != nil {
					t.Errorf("批量写失败: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// 每一批要么全在、要么全不在（这里都成功了，所以应该全在）
	for g := 0; g < gors; g++ {
		for b := 0; b < batches; b++ {
			for j := 0; j < perBatch; j++ {
				mustGet(t, db, fmt.Sprintf("g%d-b%03d-k%d", g, b, j), "v")
			}
		}
	}
	t.Logf("%d 个 goroutine × %d 批 × %d 条 = %d 条记录，全部正确",
		gors, batches, perBatch, gors*batches*perBatch)
}

// TestConcurrentCorrectness 是并发版的对拍测试。
//
// 每个 goroutine 负责一段互不重叠的 key 空间，各自维护自己的 map，
// 这样既能并发压测，又能精确断言。
func TestConcurrentCorrectness(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        16 << 10,
		SSTableSize:         24 << 10,
		LevelBaseSize:       64 << 10,
		L0CompactionTrigger: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const gors, ops = 6, 4000
	goldens := make([]map[string]string, gors)
	var wg sync.WaitGroup

	for g := 0; g < gors; g++ {
		goldens[g] = make(map[string]string)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			golden := goldens[id]
			rng := rand.New(rand.NewSource(int64(id) * 7919))

			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("g%d-k%04d", id, rng.Intn(300))
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5:
					val := fmt.Sprintf("v%d-%d", id, i)
					if err := db.Put([]byte(key), []byte(val)); err != nil {
						t.Errorf("Put 失败: %v", err)
						return
					}
					golden[key] = val
				case 6, 7:
					if err := db.Delete([]byte(key)); err != nil {
						t.Errorf("Delete 失败: %v", err)
						return
					}
					delete(golden, key)
				default:
					got, err := db.Get([]byte(key))
					want, exists := golden[key]
					if exists {
						if err != nil || string(got) != want {
							t.Errorf("goroutine %d 第 %d 步：%q = (%q,%v)，期望 %q",
								id, i, key, got, err, want)
							return
						}
					} else if err == nil {
						t.Errorf("goroutine %d 第 %d 步：%q 不应存在，却返回 %q", id, i, key, got)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	checkInvariants(t, db)

	// 收尾：所有 goroutine 的数据合起来做一次全量校验
	total := 0
	for g := 0; g < gors; g++ {
		for k, v := range goldens[g] {
			mustGet(t, db, k, v)
			total++
		}
	}
	t.Logf("%d 个 goroutine 并发 %d 次操作，最终 %d 个存活 key 全部正确",
		gors, ops, total)
}

// TestBackgroundErrorPropagates 验证后台出错时前台能感知到。
func TestBackgroundErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))

	// 手动注入一个后台错误
	db.bg.mu.Lock()
	db.bg.err = errors.New("模拟的后台故障")
	db.bg.mu.Unlock()

	if err := db.Put([]byte("k2"), []byte("v")); err == nil {
		t.Error("后台出错后，写入应该报错而不是继续")
	} else {
		t.Logf("前台正确感知到后台故障：%v", err)
	}
}

func BenchmarkConcurrentPut(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 4 << 20, MaxMemTables: 4})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	b.ReportAllocs()
	b.ResetTimer()

	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if err := db.Put([]byte(fmt.Sprintf("key%013d", i)), val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConcurrentGet(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 50000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), val)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			db.Get([]byte(fmt.Sprintf("key%06d", rng.Intn(n))))
		}
	})
}
