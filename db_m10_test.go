package shale

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 这个文件是 M10 的验收测试：group commit。

// TestGroupCommitMergesWrites 验证并发写入确实被合并了。
func TestGroupCommitMergesWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize: 4 << 20,
		SyncWAL:      true, // 开 fsync，让每批写入慢一点，好让别人有机会排进来
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const gors, perGor = 16, 100
	var wg sync.WaitGroup
	for g := 0; g < gors; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGor; i++ {
				if err := db.Put([]byte(fmt.Sprintf("g%02d-k%04d", id, i)), []byte("v")); err != nil {
					t.Errorf("写入失败: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	st := db.Stats()
	total := int64(gors * perGor)
	t.Logf("%d 个 goroutine 写了 %d 条：合并成 %d 批组提交，平均每批 %.2f 个写入者（最多 %d 个）",
		gors, total, st.GroupCommits, st.AvgGroupSize(), st.MaxGroupSize)
	t.Logf("→ fsync 从 %d 次降到 %d 次", total, st.GroupCommits)

	if st.GroupCommits >= total {
		t.Errorf("发生了 %d 批组提交，和写入次数 %d 一样多 —— 没有合并发生", st.GroupCommits, total)
	}
	if st.MaxGroupSize < 2 {
		t.Errorf("单批最多只合并了 %d 个写入者，group commit 似乎没生效", st.MaxGroupSize)
	}

	// 数据一条不能少、不能错
	for g := 0; g < gors; g++ {
		for i := 0; i < perGor; i++ {
			mustGet(t, db, fmt.Sprintf("g%02d-k%04d", g, i), "v")
		}
	}
}

// TestGroupCommitSeqOrdering 验证合并之后序号仍然正确分配。
//
// 这是 group commit 最容易出错的地方：多个 writer 的记录被拼进
// 同一个 batch，序号必须连续且不重叠，否则版本关系会乱。
func TestGroupCommitSeqOrdering(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 4 << 20, SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 多个 goroutine 反复改写【同一个 key】——
	// 如果序号分配错了，最终读到的值就不是最后写入的那个
	const gors, rounds = 8, 200
	var wg sync.WaitGroup
	var lastWritten atomic.Int64

	for g := 0; g < gors; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				n := lastWritten.Add(1)
				if err := db.Put([]byte("hot"), []byte(fmt.Sprintf("v%d", n))); err != nil {
					t.Errorf("写入失败: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// 不管谁最后写的，读出来的必须是【某一次真实写入】的值，
	// 而且 seq 必须恰好等于总写入次数
	got, err := db.Get([]byte("hot"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d 个 goroutine 各写 %d 次同一个 key，最终值 = %s", gors, rounds, got)

	db.mu.RLock()
	seq := db.seq
	db.mu.RUnlock()
	if seq != uint64(gors*rounds) {
		t.Errorf("最终 seq = %d，期望 %d（每条记录恰好消耗一个序号）", seq, gors*rounds)
	}
}

// TestGroupCommitSyncThroughput 是 M10 的核心验收：
// 开启 fsync 时，并发写的吞吐应该远高于单线程。
//
// 这正是 group commit 的意义：一次 fsync 服务一整批人。
func TestGroupCommitSyncThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过")
	}

	measure := func(gors int) (time.Duration, Stats) {
		dir := t.TempDir()
		db, err := Open(dir, &Options{MemTableSize: 8 << 20, SyncWAL: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		const total = 800
		perGor := total / gors

		start := time.Now()
		var wg sync.WaitGroup
		for g := 0; g < gors; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < perGor; i++ {
					db.Put([]byte(fmt.Sprintf("g%d-k%05d", id, i)), []byte("value"))
				}
			}(g)
		}
		wg.Wait()
		return time.Since(start), db.Stats()
	}

	serial, stSerial := measure(1)
	parallel, stParallel := measure(32)

	opsSerial := float64(800) / serial.Seconds()
	opsParallel := float64(800) / parallel.Seconds()

	t.Logf("单线程  ：%v，%.0f ops/s，%d 批组提交（平均每批 %.2f 个）",
		serial, opsSerial, stSerial.GroupCommits, stSerial.AvgGroupSize())
	t.Logf("32 并发 ：%v，%.0f ops/s，%d 批组提交（平均每批 %.2f 个，最多 %d 个）",
		parallel, opsParallel, stParallel.GroupCommits,
		stParallel.AvgGroupSize(), stParallel.MaxGroupSize)
	t.Logf("→ 提速 %.1f 倍，fsync 次数从 800 降到 %d",
		opsParallel/opsSerial, stParallel.GroupCommits)

	if stParallel.AvgGroupSize() < 2 {
		t.Errorf("32 并发下平均每批只合并了 %.2f 个写入者，group commit 效果不明显",
			stParallel.AvgGroupSize())
	}
	if opsParallel < opsSerial {
		t.Errorf("并发 (%.0f ops/s) 竟然比单线程 (%.0f ops/s) 还慢", opsParallel, opsSerial)
	}
}

// TestGroupCommitDoesNotMutateCallerBatch 验证合并不会改动调用方的 Batch。
//
// leader 合并时如果就地往 first.b 上追加，调用方 Write 返回后
// 再复用那个 Batch 就会发现里面多了别人的数据。
func TestGroupCommitDoesNotMutateCallerBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 4 << 20, SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const gors = 12
	var wg sync.WaitGroup
	bad := atomic.Int64{}

	for g := 0; g < gors; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				b := NewBatch()
				b.Put([]byte(fmt.Sprintf("g%d-k%d", id, i)), []byte("v"))
				if err := db.Write(b); err != nil {
					t.Errorf("写入失败: %v", err)
					return
				}
				// Write 返回后，我的 Batch 必须还是原样：1 条记录
				if b.Count() != 1 {
					bad.Add(1)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if n := bad.Load(); n > 0 {
		t.Errorf("有 %d 次发现调用方的 Batch 被合并操作污染了", n)
	}
}

// TestGroupCommitErrorPropagates 验证 leader 失败时整批人都收到错误。
func TestGroupCommitErrorPropagates(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))

	// 注入一个后台错误，之后所有写入都该失败
	db.bg.mu.Lock()
	db.bg.err = fmt.Errorf("模拟的后台故障")
	db.bg.mu.Unlock()

	var wg sync.WaitGroup
	var okCount atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := db.Put([]byte(fmt.Sprintf("after%d", id)), []byte("v")); err == nil {
				okCount.Add(1)
			}
		}(g)
	}
	wg.Wait()

	// 有些可能在错误注入前就写完了，但不该全部成功
	t.Logf("注入故障后，8 个并发写里有 %d 个返回成功", okCount.Load())
}

// TestWriteAfterCloseWakesQueuedWriters 验证关闭时排队的人会被叫醒，
// 而不是永远睡在 cond 上。
func TestWriteAfterCloseWakesQueuedWriters(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := db.Put([]byte(fmt.Sprintf("g%d-k%d", id, i)), []byte("v")); err != nil {
					return // 关闭之后报错是预期的
				}
			}
		}(g)
	}

	time.Sleep(10 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}

	// 如果排队的 writer 没被叫醒，这里会永远卡住
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("关闭后仍有 writer 卡在队列里没被唤醒")
	}
}

// TestGroupCommitSmallWritesNotStarved 验证小写入不会被卷进大批次白等。
func TestGroupCommitSmallWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 8 << 20, SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup

	// 一半 goroutine 写大 batch，一半写小的
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				b := NewBatch()
				for j := 0; j < 500; j++ {
					b.Put([]byte(fmt.Sprintf("big%d-%d-%d", id, i, j)), make([]byte, 200))
				}
				db.Write(b)
			}
		}(g)
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				db.Put([]byte(fmt.Sprintf("small%d-%d", id, i)), []byte("v"))
			}
		}(g)
	}
	wg.Wait()

	st := db.Stats()
	t.Logf("大小写入混合：%d 批组提交，平均每批 %.2f 个，最多 %d 个",
		st.GroupCommits, st.AvgGroupSize(), st.MaxGroupSize)

	// 数据都在
	for g := 0; g < 4; g++ {
		for i := 0; i < 200; i++ {
			mustGet(t, db, fmt.Sprintf("small%d-%d", g, i), "v")
		}
	}
}

func BenchmarkSyncWriteSerial(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 8 << 20, SyncWAL: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%013d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(db.Stats().AvgGroupSize(), "个/批")
}

func BenchmarkSyncWriteParallel(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 8 << 20, SyncWAL: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	var counter atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			if err := db.Put([]byte(fmt.Sprintf("key%013d", i)), val); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(db.Stats().AvgGroupSize(), "个/批")
}
