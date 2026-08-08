package shale

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// 这个文件是 M2 的验收测试：数据要能活过重启。
//
// 「重启」在测试里有两种模拟方式，区别很重要：
//
//	正常关闭 —— db.Close() 之后重开
//	模拟崩溃 —— 【不调用 Close】，直接丢弃 db 对象再重开。
//	           这才是真正考验 WAL 的场景：没有任何收尾工作，
//	           全靠写入时已经落到日志里的内容。

// reopen 正常关闭后重新打开。
func reopen(t *testing.T, db *DB, dir string) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	nd, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	return nd
}

// crashReopen 模拟进程被 kill -9：不调用 Close，直接重开。
func crashReopen(t *testing.T, dir string, opts *Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("崩溃后重新打开失败: %v", err)
	}
	return db
}

func TestDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	db.Delete([]byte("key050"))

	db = reopen(t, db, dir)
	defer db.Close()

	if db.RecoveredEntries() != 101 {
		t.Errorf("恢复了 %d 条记录，期望 101（100 次写入 + 1 次删除）", db.RecoveredEntries())
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		if i == 50 {
			mustMiss(t, db, key) // 删除也要被正确重放
			continue
		}
		mustGet(t, db, key, fmt.Sprintf("val%03d", i))
	}
}

// TestDataSurvivesCrash 是 M2 的核心验收：不调用 Close 也不能丢数据。
func TestDataSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	// 故意不 Close —— 模拟 kill -9

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()

	for i := 0; i < 500; i++ {
		mustGet(t, db2, fmt.Sprintf("k%04d", i), "value")
	}
	t.Logf("崩溃后恢复了 %d 条记录", db2.RecoveredEntries())
}

// TestSeqSurvivesRestart 验证序号在重启后继续递增。
//
// 这一条极其关键：如果重启后 seq 从 0 重新开始，
// 新写入的记录会被判定为【比磁盘上的老数据还旧】，读出来的就是老值。
func TestSeqSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))
	db.Put([]byte("k"), []byte("v3"))
	seqBefore := db.seq

	db = reopen(t, db, dir)
	defer db.Close()

	if db.seq != seqBefore {
		t.Errorf("重启后 seq = %d，期望 %d", db.seq, seqBefore)
	}
	mustGet(t, db, "k", "v3")

	// 重启后再写，必须能覆盖旧值
	db.Put([]byte("k"), []byte("v4"))
	mustGet(t, db, "k", "v4")

	if db.seq <= seqBefore {
		t.Errorf("新写入后 seq = %d，应该大于 %d", db.seq, seqBefore)
	}
}

// TestMultipleRestarts 验证反复重启不出问题（每次都在上次的基础上继续）。
func TestMultipleRestarts(t *testing.T) {
	dir := t.TempDir()

	var db *DB
	for round := 0; round < 5; round++ {
		var err error
		db, err = Open(dir, nil)
		if err != nil {
			t.Fatalf("第 %d 轮打开失败: %v", round, err)
		}
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("r%d-k%d", round, i)
			if err := db.Put([]byte(key), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		// 前几轮的数据都要还在
		for r := 0; r <= round; r++ {
			for i := 0; i < 20; i++ {
				mustGet(t, db, fmt.Sprintf("r%d-k%d", r, i), "v")
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	db, _ = Open(dir, nil)
	defer db.Close()
	if db.RecoveredEntries() != 100 {
		t.Errorf("最终恢复 %d 条，期望 100", db.RecoveredEntries())
	}
}

// TestBatchAtomicityAcrossCrash 验证批量写的原子性能挺过崩溃：
// 一个 batch 要么整批出现，要么整批消失，不会只恢复一半。
func TestBatchAtomicityAcrossCrash(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 50; i++ {
		b := NewBatch()
		for j := 0; j < 10; j++ {
			b.Put([]byte(fmt.Sprintf("b%02d-k%d", i, j)), []byte("v"))
		}
		if err := db.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	// 不 Close

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()

	// 检查每一批：要么 10 条全在，要么 10 条全不在
	for i := 0; i < 50; i++ {
		present := 0
		for j := 0; j < 10; j++ {
			if _, err := db2.Get([]byte(fmt.Sprintf("b%02d-k%d", i, j))); err == nil {
				present++
			}
		}
		if present != 0 && present != 10 {
			t.Errorf("第 %d 批只恢复了 %d/10 条 —— 原子性被破坏", i, present)
		}
	}
}

// TestTruncatedWALTail 模拟崩溃在写日志的中途：
// 手动截掉文件尾部若干字节，验证能恢复出所有完整的记录。
func TestTruncatedWALTail(t *testing.T) {
	for _, cut := range []int64{1, 3, 7, 20, 100} {
		t.Run(fmt.Sprintf("截掉%d字节", cut), func(t *testing.T) {
			dir := t.TempDir()

			db, _ := Open(dir, nil)
			const n = 200
			for i := 0; i < n; i++ {
				db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("value"))
			}
			db.Close()

			// 找到 WAL 文件，从尾部砍掉一截
			walFile := filepath.Join(dir, "000001.log")
			info, err := os.Stat(walFile)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() <= cut {
				t.Skip("日志太小")
			}
			if err := os.Truncate(walFile, info.Size()-cut); err != nil {
				t.Fatal(err)
			}

			db2 := crashReopen(t, dir, nil)
			defer db2.Close()

			// 前面的记录必须完好；尾部丢几条是可以接受的
			recovered := db2.RecoveredEntries()
			if recovered > n {
				t.Fatalf("恢复了 %d 条，超过了原有的 %d 条", recovered, n)
			}
			if recovered < n-5 {
				t.Errorf("只恢复了 %d 条，丢失过多（原有 %d 条，只截掉了 %d 字节）",
					recovered, n, cut)
			}
			for i := 0; i < recovered; i++ {
				mustGet(t, db2, fmt.Sprintf("k%04d", i), "value")
			}

			// 关键：截断后还能继续正常写入和读取
			if err := db2.Put([]byte("after-crash"), []byte("ok")); err != nil {
				t.Fatalf("崩溃恢复后无法继续写入: %v", err)
			}
			mustGet(t, db2, "after-crash", "ok")
		})
	}
}

// TestWriteAfterRecoveryPersists 验证「截断 → 续写」之后的数据也能再次恢复。
// 这一条要是错了，说明续写时的块内偏移算错了。
func TestWriteAfterRecoveryPersists(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("old%03d", i)), []byte("v"))
	}
	// 不 Close，模拟崩溃

	db2 := crashReopen(t, dir, nil)
	for i := 0; i < 100; i++ {
		db2.Put([]byte(fmt.Sprintf("new%03d", i)), []byte("v"))
	}
	// 再次崩溃

	db3 := crashReopen(t, dir, nil)
	defer db3.Close()

	for i := 0; i < 100; i++ {
		mustGet(t, db3, fmt.Sprintf("old%03d", i), "v")
		mustGet(t, db3, fmt.Sprintf("new%03d", i), "v")
	}
}

// TestLargeValuesSurviveRestart 验证跨块的大记录也能正确恢复。
func TestLargeValuesSurviveRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	sizes := []int{100, 1 << 10, 32 << 10, 100 << 10, 300 << 10}
	for i, size := range sizes {
		val := make([]byte, size)
		for j := range val {
			val[j] = byte('a' + i)
		}
		if err := db.Put([]byte(fmt.Sprintf("big%d", i)), val); err != nil {
			t.Fatalf("写 %d 字节失败: %v", size, err)
		}
	}

	db = reopen(t, db, dir)
	defer db.Close()

	for i, size := range sizes {
		got, err := db.Get([]byte(fmt.Sprintf("big%d", i)))
		if err != nil {
			t.Fatalf("big%d (%d 字节) 恢复失败: %v", i, size, err)
		}
		if len(got) != size {
			t.Errorf("big%d 长度 = %d，期望 %d", i, len(got), size)
		}
		for j := range got {
			if got[j] != byte('a'+i) {
				t.Fatalf("big%d 第 %d 字节损坏", i, j)
				break
			}
		}
	}
}

func TestCorruptWALStopsReplay(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value"))
	}
	db.Close()

	// 把日志中间的某个字节改坏
	walFile := filepath.Join(dir, "000001.log")
	data, err := os.ReadFile(walFile)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(walFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()

	// 应该恢复出损坏点之前的数据，并给出告警
	if db2.RecoverWarning() == nil {
		t.Error("日志损坏时应该有告警")
	} else {
		t.Logf("告警：%v", db2.RecoverWarning())
	}
	if db2.RecoveredEntries() == 0 {
		t.Error("损坏点之前的数据应该能恢复出来")
	}
	if db2.RecoveredEntries() >= 100 {
		t.Error("损坏点之后的数据不应该被恢复")
	}
	t.Logf("从损坏的日志里恢复了 %d/100 条", db2.RecoveredEntries())
}

func TestSyncWALOption(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, &Options{SyncWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	db2 := crashReopen(t, dir, &Options{SyncWAL: true})
	defer db2.Close()
	for i := 0; i < 50; i++ {
		mustGet(t, db2, fmt.Sprintf("k%d", i), "v")
	}
}

func TestReadOnlyRejectsWrites(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	db.Put([]byte("k"), []byte("v"))
	db.Close()

	ro, err := Open(dir, &Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	mustGet(t, ro, "k", "v") // 读得到
	if err := ro.Put([]byte("k2"), []byte("v")); !errors.Is(err, ErrReadOnly) {
		t.Errorf("只读模式写入应返回 ErrReadOnly，得到 %v", err)
	}
}

// TestAgainstGoMapWithRestarts 是对拍测试的持久化版本：
// 在随机操作过程中【穿插重启】，每次重启后都要和 map 完全一致。
//
// 这是 M1 那个安全网的自然延伸 —— 以后每加一层存储都会再跑一次。
func TestAgainstGoMapWithRestarts(t *testing.T) {
	dir := t.TempDir()
	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260809))

	const (
		rounds      = 10
		opsPerRound = 3000
		keySpan     = 500
	)

	for round := 0; round < rounds; round++ {
		db, err := Open(dir, nil)
		if err != nil {
			t.Fatalf("第 %d 轮打开失败: %v", round, err)
		}

		// 每轮开头先做一次全量校验：上一轮的数据必须原样恢复
		for i := 0; i < keySpan; i++ {
			key := fmt.Sprintf("key%04d", i)
			got, err := db.Get([]byte(key))
			want, exists := golden[key]
			if exists {
				if err != nil {
					t.Fatalf("第 %d 轮开头：%q 应存在（%q），却返回 %v", round, key, want, err)
				}
				if string(got) != want {
					t.Fatalf("第 %d 轮开头：%q = %q，期望 %q", round, key, got, want)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("第 %d 轮开头：%q 不应存在，却返回 %q", round, key, got)
			}
		}

		for i := 0; i < opsPerRound; i++ {
			key := fmt.Sprintf("key%04d", rng.Intn(keySpan))
			switch rng.Intn(10) {
			case 0, 1, 2, 3, 4: // 50% 写
				val := fmt.Sprintf("r%d-i%d", round, i)
				if err := db.Put([]byte(key), []byte(val)); err != nil {
					t.Fatal(err)
				}
				golden[key] = val
			case 5, 6: // 20% 删
				if err := db.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
				delete(golden, key)
			default: // 30% 读
				got, err := db.Get([]byte(key))
				want, exists := golden[key]
				if exists {
					if err != nil || string(got) != want {
						t.Fatalf("第 %d 轮第 %d 步：%q = (%q,%v)，期望 %q", round, i, key, got, err, want)
					}
				} else if !errors.Is(err, ErrNotFound) {
					t.Fatalf("第 %d 轮第 %d 步：%q 不应存在，却返回 %q", round, i, key, got)
				}
			}
		}

		// 一半轮次正常关闭，一半直接崩溃 —— 两条路径都要覆盖
		if round%2 == 0 {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		// 奇数轮不 Close，直接进入下一轮重开
	}

	db, _ := Open(dir, nil)
	defer db.Close()
	t.Logf("%d 轮重启后，map 里有 %d 个 key，最后一次恢复了 %d 条记录",
		rounds, len(golden), db.RecoveredEntries())
}

func BenchmarkPutWithWAL(b *testing.B) {
	db, err := Open(b.TempDir(), nil)
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
}

func BenchmarkPutWithSyncWAL(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{SyncWAL: true})
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
}
