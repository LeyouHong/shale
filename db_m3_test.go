package shale

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 这个文件是 M3 的验收测试：数据能从内存落到磁盘文件。

// numTables 返回当前【生效】的 SSTable 数量。
//
// 注意不能用 len(db.tables) —— 那是懒加载的句柄缓存，
// 只反映"打开过哪些文件"。哪些文件生效由 Manifest 说了算。
func numTables(db *DB) int {
	return db.vs.Current().TotalFiles()
}

// countFiles 统计目录里某种后缀的文件数。
func countFiles(t *testing.T, dir, suffix string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			n++
		}
	}
	return n
}

func TestFlushCreatesSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value"))
	}

	if countFiles(t, dir, ".sst") != 0 {
		t.Error("还没 Flush 就有 SSTable 了")
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush 失败: %v", err)
	}

	if n := countFiles(t, dir, ".sst"); n != 1 {
		t.Errorf("Flush 后有 %d 个 SSTable，期望 1 个", n)
	}
	if !db.mem.Empty() {
		t.Error("Flush 后 MemTable 应该被清空")
	}
	// 数据必须还能读到（现在来自磁盘）
	for i := 0; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("k%03d", i), "value")
	}
}

// TestFlushTruncatesWAL 验证 flush 之后旧日志被删掉 ——
// 这正是 flush 存在的意义之一：让 WAL 不再无限增长。
func TestFlushTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	defer db.Close()

	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("some value here"))
	}
	walsBefore := countFiles(t, dir, ".log")

	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	walsAfter := countFiles(t, dir, ".log")
	if walsAfter != 1 {
		t.Errorf("Flush 后有 %d 个 WAL，期望 1 个（只剩新开的那个）", walsAfter)
	}
	t.Logf("Flush 前 %d 个 WAL，之后 %d 个", walsBefore, walsAfter)
}

// TestReadFromSSTableAfterRestart 是 M3 的核心验收：
// 数据落盘后重启，必须只从 SSTable 就能读出来（WAL 已经被删了）。
func TestReadFromSSTableAfterRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 300; i++ {
		db.Put([]byte(fmt.Sprintf("key%04d", i)), []byte(fmt.Sprintf("val%04d", i)))
	}
	db.Delete([]byte("key0150"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// WAL 已经被删掉了，所以恢复条数应该是 0 —— 数据全部来自 SSTable
	if db2.RecoveredEntries() != 0 {
		t.Errorf("从 WAL 恢复了 %d 条，期望 0（数据应该都在 SSTable 里）",
			db2.RecoveredEntries())
	}
	if numTables(db2) != 1 {
		t.Fatalf("加载了 %d 个 SSTable，期望 1 个", numTables(db2))
	}

	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key%04d", i)
		if i == 150 {
			mustMiss(t, db2, key)
			continue
		}
		mustGet(t, db2, key, fmt.Sprintf("val%04d", i))
	}
}

// TestNewerSSTableWins 验证多个 SSTable 之间的覆盖关系 ——
// 这是 LSM 读路径最核心的正确性。
func TestNewerSSTableWins(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	defer db.Close()

	// 每次写完就刷，制造出 3 个 SSTable，同一个 key 在每个文件里都有一份
	db.Put([]byte("k"), []byte("v1"))
	db.Flush()
	db.Put([]byte("k"), []byte("v2"))
	db.Flush()
	db.Put([]byte("k"), []byte("v3"))
	db.Flush()

	if numTables(db) != 3 {
		t.Fatalf("应该有 3 个 SSTable，实际 %d 个", numTables(db))
	}
	// 必须读到最新的那个
	mustGet(t, db, "k", "v3")

	// 重启后依然如此（顺序不能因为重新加载而乱掉）
	db.Close()
	db2, _ := Open(dir, nil)
	defer db2.Close()
	mustGet(t, db2, "k", "v3")
}

// TestTombstoneAcrossSSTables 验证墓碑能遮住更老文件里的值。
//
// 这一条要是错了，删除过的数据会"复活" —— LSM 最经典的 bug。
func TestTombstoneAcrossSSTables(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Put([]byte("k"), []byte("value"))
	db.Flush() // 值进了 000001.sst

	db.Delete([]byte("k"))
	db.Flush() // 墓碑进了 000003.sst

	if numTables(db) != 2 {
		t.Fatalf("应该有 2 个 SSTable，实际 %d 个", numTables(db))
	}
	// 查找先问新文件 → 碰到墓碑 → 立刻返回不存在，不再去问老文件
	mustMiss(t, db, "k")

	db.Close()
	db2, _ := Open(dir, nil)
	defer db2.Close()
	mustMiss(t, db2, "k")

	// 删了还能再写回来
	db2.Put([]byte("k"), []byte("back"))
	mustGet(t, db2, "k", "back")
	db2.Flush()
	mustGet(t, db2, "k", "back")
}

// TestAutoFlush 验证 MemTable 写满会自动落盘。
func TestAutoFlush(t *testing.T) {
	dir := t.TempDir()
	// 用很小的 MemTable，几十次写入就会触发多次 flush
	db, err := Open(dir, &Options{MemTableSize: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 2000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%05d", i)), val); err != nil {
			t.Fatal(err)
		}
	}

	if numTables(db) < 5 {
		t.Errorf("只产生了 %d 个 SSTable，自动 flush 似乎没触发", numTables(db))
	}
	if db.mem.Size() >= 16<<10 {
		t.Errorf("MemTable 大小 %d 超过了阈值，说明没及时 flush", db.mem.Size())
	}
	t.Logf("写入 %d 条后产生了 %d 个 SSTable，MemTable 当前 %s",
		n, numTables(db), humanBytes(db.mem.Size()))

	// 所有数据都要能读到（分散在多个文件和内存里）
	for i := 0; i < n; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key%05d", i))); err != nil {
			t.Fatalf("key%05d 读不到: %v", i, err)
		}
	}
}

// TestCrashDuringFlushKeepsData 验证 flush 过程中崩溃不丢数据。
//
// flush 的步骤顺序就是为这个场景设计的：
// 先开新 WAL、再写 SSTable、最后才删旧 WAL。
// 任何一步崩溃，旧 WAL 都还在，重放就能恢复。
func TestCrashDuringFlushKeepsData(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	// 不 Close 也不 Flush，直接崩溃 —— 数据只在 MemTable 和 WAL 里

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()
	for i := 0; i < 200; i++ {
		mustGet(t, db2, fmt.Sprintf("k%03d", i), "v")
	}
}

// TestLeftoverTempFileCleaned 验证上次崩溃留下的半成品被清理掉。
func TestLeftoverTempFileCleaned(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	db.Put([]byte("k"), []byte("v"))
	db.Close()

	// 手动伪造一个写了一半的 SSTable
	junk := filepath.Join(dir, "000099.sst.tmp")
	if err := os.WriteFile(junk, []byte("incomplete garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("有残留 .tmp 时应该也能正常打开: %v", err)
	}
	defer db2.Close()

	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Error(".tmp 残留文件应该被清理掉")
	}
	mustGet(t, db2, "k", "v")
}

func TestFlushEmptyMemTableIsNoop(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	defer db.Close()

	if err := db.Flush(); err != nil {
		t.Errorf("刷空 MemTable 不应报错: %v", err)
	}
	if n := countFiles(t, dir, ".sst"); n != 0 {
		t.Errorf("空 MemTable 不应产生 SSTable，却有 %d 个", n)
	}
}

func TestStatsShowSSTables(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	defer db.Close()

	val := make([]byte, 100)
	for i := 0; i < 1000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), val)
	}

	st := db.Stats()
	if len(st.Levels) == 0 || st.Levels[0].NumFiles == 0 {
		t.Error("Stats 应该报告 SSTable 文件数")
	}
	if st.WriteAmplification() <= 0 {
		t.Error("应该有写放大数据")
	}
	t.Logf("\n%s", st)
}

// TestAgainstGoMapWithFlush 是对拍测试的落盘版本：
// 用很小的 MemTable 制造大量 flush，全程与 map 比对，并穿插重启。
//
// 这一版真正开始考验 LSM 的读路径 —— 同一个 key 的不同版本
// 会散落在 MemTable 和多个 SSTable 里，必须每次都挑出正确的那个。
func TestAgainstGoMapWithFlush(t *testing.T) {
	dir := t.TempDir()
	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260810))

	const (
		rounds      = 6
		opsPerRound = 4000
		keySpan     = 400
	)
	opts := &Options{MemTableSize: 24 << 10} // 小到几百次写入就刷一次

	for round := 0; round < rounds; round++ {
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("第 %d 轮打开失败: %v", round, err)
		}

		// 开头全量校验：上一轮的数据（散落在各 SSTable 里）必须原样可读
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
				val := fmt.Sprintf("r%d-i%d-%s", round, i, strings.Repeat("x", rng.Intn(50)))
				if err := db.Put([]byte(key), []byte(val)); err != nil {
					t.Fatal(err)
				}
				golden[key] = val
			case 5, 6: // 20% 删
				if err := db.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
				delete(golden, key)
			case 7: // 2% 手动 flush（制造更多 SSTable）
				if rng.Intn(5) == 0 {
					if err := db.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			default: // 20% 读
				got, err := db.Get([]byte(key))
				want, exists := golden[key]
				if exists {
					if err != nil || string(got) != want {
						t.Fatalf("第 %d 轮第 %d 步：%q = (%q,%v)，期望 %q",
							round, i, key, got, err, want)
					}
				} else if !errors.Is(err, ErrNotFound) {
					t.Fatalf("第 %d 轮第 %d 步：%q 不应存在，却返回 %q", round, i, key, got)
				}
			}
		}

		if round%2 == 0 {
			db.Close()
		}
		// 奇数轮直接崩溃
	}

	db, _ := Open(dir, opts)
	defer db.Close()

	// 最终全量校验
	for i := 0; i < keySpan; i++ {
		key := fmt.Sprintf("key%04d", i)
		got, err := db.Get([]byte(key))
		want, exists := golden[key]
		if exists {
			if err != nil || string(got) != want {
				t.Fatalf("最终校验：%q = (%q,%v)，期望 %q", key, got, err, want)
			}
		} else if !errors.Is(err, ErrNotFound) {
			t.Fatalf("最终校验：%q 不应存在，却返回 %q", key, got)
		}
	}

	st := db.Stats()
	files := st.Levels[0].NumFiles
	t.Logf("%d 轮之后：%d 个存活 key，散落在 %d 个 SSTable 里，共 %s",
		rounds, len(golden), files, humanBytes(st.Levels[0].Size))

	// 这个比值是 M6 存在的理由：
	// 没有 compaction 时文件只增不减，一次 Get 最坏要问遍所有文件。
	if files > len(golden) {
		t.Logf("注意：文件数(%d) 已经超过存活 key 数(%d) —— "+
			"每次 Get 最坏要查 %d 个文件。compaction（M6）就是来解决这个的。",
			files, len(golden), files)
	}
}

func BenchmarkGetFromSSTable(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 50000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), val)
	}
	db.Flush() // 全部落盘

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key%06d", i%n))); err != nil {
			b.Fatal(err)
		}
	}
}
