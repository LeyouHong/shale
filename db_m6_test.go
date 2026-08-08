package shale

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// 这个文件是 M6 的验收测试：compaction 之后文件数下降、垃圾被清理、数据不变。

func TestCompactionReducesFileCount(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        8 << 10,
		L0CompactionTrigger: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%05d", i)), val); err != nil {
			t.Fatal(err)
		}
	}

	st := db.Stats()
	l0 := st.Levels[0].NumFiles
	if l0 >= db.opts.L0CompactionTrigger {
		t.Errorf("L0 有 %d 个文件，不该达到触发阈值 %d —— compaction 没跟上",
			l0, db.opts.L0CompactionTrigger)
	}
	if db.compactionCount == 0 {
		t.Error("应该发生过 compaction")
	}
	t.Logf("写入 %d 条：flush %d 次、compaction %d 次，最终 L0=%d 个、L1=%d 个文件",
		n, db.flushCount, db.compactionCount,
		st.Levels[0].NumFiles, levelFiles(st, 1))

	// 数据一条都不能少
	for i := 0; i < n; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key%05d", i))); err != nil {
			t.Fatalf("key%05d 在 compaction 后丢失: %v", i, err)
		}
	}
}

func levelFiles(st Stats, level int) int {
	for _, l := range st.Levels {
		if l.Level == level {
			return l.NumFiles
		}
	}
	return 0
}

// TestCompactionDropsOldVersions 验证被覆盖的旧版本会被真正清掉。
//
// 这是 compaction 最直接的价值：空间放大被消除。
func TestCompactionDropsOldVersions(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 4 << 10})
	defer db.Close()

	// 反复改写同一批 key，制造大量旧版本
	const keys, rounds = 50, 100
	for r := 0; r < rounds; r++ {
		for k := 0; k < keys; k++ {
			db.Put([]byte(fmt.Sprintf("k%03d", k)), []byte(fmt.Sprintf("round%03d", r)))
		}
	}

	sizeBefore := totalSize(db.Stats())
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	sizeAfter := totalSize(db.Stats())

	// 50 个 key 写了 100 轮 = 5000 条记录，合并后应该只剩 50 条
	entries := countEntries(t, db)
	if entries != keys {
		t.Errorf("compaction 后剩 %d 条记录，期望 %d 条（每个 key 只留最新版本）",
			entries, keys)
	}
	if sizeAfter >= sizeBefore {
		t.Errorf("compaction 后占用 %d 字节，没比之前的 %d 小", sizeAfter, sizeBefore)
	}
	t.Logf("%d 个 key 写了 %d 轮（%d 条记录）：compaction 前 %s，之后 %s，剩 %d 条记录",
		keys, rounds, keys*rounds, humanBytes(sizeBefore), humanBytes(sizeAfter), entries)

	// 值必须是最后一轮写的
	for k := 0; k < keys; k++ {
		mustGet(t, db, fmt.Sprintf("k%03d", k), fmt.Sprintf("round%03d", rounds-1))
	}
}

func totalSize(st Stats) int64 {
	var n int64
	for _, l := range st.Levels {
		n += l.Size
	}
	return n
}

// countEntries 数一遍当前有多少条【用户可见】的记录。
func countEntries(t *testing.T, db *DB) int {
	t.Helper()
	it, err := db.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		n++
	}
	return n
}

// TestCompactionDropsTombstonesAtBottom 验证墓碑在最底层会被彻底清除。
func TestCompactionDropsTombstonesAtBottom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 4 << 10})
	defer db.Close()

	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value"))
	}
	db.Flush()
	// 全删掉
	for i := 0; i < 200; i++ {
		db.Delete([]byte(fmt.Sprintf("k%03d", i)))
	}

	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}

	// 数据和墓碑应该一起消失，磁盘几乎清空
	if n := countEntries(t, db); n != 0 {
		t.Errorf("全删之后还剩 %d 条记录", n)
	}
	size := totalSize(db.Stats())
	t.Logf("200 条数据 + 200 个墓碑，compaction 之后磁盘只剩 %s", humanBytes(size))
	if size > 4<<10 {
		t.Errorf("墓碑似乎没被清理，还占着 %s", humanBytes(size))
	}
	for i := 0; i < 200; i++ {
		mustMiss(t, db, fmt.Sprintf("k%03d", i))
	}
}

// TestTombstoneNotDroppedAboveBottom 验证【非】最底层的墓碑必须保留。
//
// 这是 compaction 最危险的一处：早一步丢掉墓碑，
// 更底层的旧数据就会"复活"。
func TestTombstoneNotDroppedAboveBottom(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 4 << 10, L0CompactionTrigger: 2})
	defer db.Close()

	// 先造出一个 L1（老数据沉底）
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old-value"))
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	if db.vs.Current().NumFiles(1) == 0 {
		t.Skip("没有形成 L1，跳过")
	}

	// 再删掉其中一半
	for i := 0; i < 50; i++ {
		db.Delete([]byte(fmt.Sprintf("k%03d", i)))
	}
	db.Flush()

	// 此刻墓碑在 L0、旧值在 L1。查询必须看不到被删的那些。
	for i := 0; i < 50; i++ {
		mustMiss(t, db, fmt.Sprintf("k%03d", i))
	}
	for i := 50; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("k%03d", i), "old-value")
	}

	// 全量合并之后仍然如此 —— 墓碑和旧值应该一起消失，而不是旧值复活
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		mustMiss(t, db, fmt.Sprintf("k%03d", i))
	}
	for i := 50; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("k%03d", i), "old-value")
	}
	if n := countEntries(t, db); n != 50 {
		t.Errorf("应剩 50 条记录，实际 %d 条", n)
	}
}

// TestCompactionKeepsDataIntact 是最直接的验收：
// compaction 前后，数据必须一模一样。
func TestCompactionKeepsDataIntact(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	defer db.Close()

	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260812))
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key%04d", rng.Intn(600))
		if rng.Intn(5) == 0 {
			db.Delete([]byte(key))
			delete(golden, key)
		} else {
			val := fmt.Sprintf("v%d", i)
			db.Put([]byte(key), []byte(val))
			golden[key] = val
		}
	}

	before := scanAll(t, db)

	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}

	after := scanAll(t, db)
	assertScan(t, after, before)

	// 再和 map 对一遍
	want := make([]string, 0, len(golden))
	for k, v := range golden {
		want = append(want, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(want)
	assertScan(t, after, want)

	t.Logf("compaction 前后都是 %d 条记录，内容完全一致", len(after))
}

// TestCompactionSurvivesRestart 验证 compaction 的结果能正确持久化。
func TestCompactionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	for i := 0; i < 2000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("value"))
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	before := scanAll(t, db)
	filesBefore := numTables(db)
	db.Close()

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if numTables(db2) != filesBefore {
		t.Errorf("重启后有 %d 个文件，期望 %d 个", numTables(db2), filesBefore)
	}
	assertScan(t, scanAll(t, db2), before)
}

// TestIteratorSurvivesCompaction 验证 compaction 期间的迭代器不受影响。
//
// 这依赖 M4 的引用计数：compaction 不改现有文件，只生成新文件，
// 旧文件要等所有读者离开才真正删盘。
func TestIteratorSurvivesCompaction(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	defer db.Close()

	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("original"))
	}
	db.Flush()

	// 开一个迭代器并读几条
	it, err := db.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	it.SeekToFirst()
	var got []string
	for i := 0; i < 10 && it.Valid(); i++ {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
		it.Next()
	}

	// 遍历到一半时做一次全量 compaction（会重写所有文件）
	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("rewritten"))
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}

	// 迭代器必须继续给出【它那一刻】的数据
	for ; it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("compaction 之后迭代器出错了: %v", err)
	}
	if len(got) != 500 {
		t.Fatalf("迭代器输出 %d 条，期望 500 条", len(got))
	}
	for i, s := range got {
		want := fmt.Sprintf("k%04d=original", i)
		if s != want {
			t.Fatalf("第 %d 条 = %q，期望 %q —— 迭代器看到了 compaction 之后的数据", i, s, want)
		}
	}
	t.Log("compaction 重写了全部文件，迭代器仍然完整看到了旧快照")
}

// TestWriteAmplification 观察写放大 —— LSM 最核心的代价指标。
func TestWriteAmplification(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 16 << 10})
	defer db.Close()

	val := make([]byte, 200)
	for i := 0; i < 5000; i++ {
		db.Put([]byte(fmt.Sprintf("key%05d", i)), val)
	}
	db.Flush()

	st := db.Stats()
	wa := st.WriteAmplification()
	t.Logf("用户写入 %s，磁盘写入 %s，写放大 %.2fx（flush %d 次、compaction %d 次）",
		humanBytes(st.UserBytesWritten), humanBytes(st.DiskBytesWritten), wa,
		db.flushCount, db.compactionCount)

	if wa < 1 {
		t.Errorf("写放大 %.2f 小于 1，统计有问题", wa)
	}
	if wa > 30 {
		t.Errorf("写放大 %.2f 偏高（超过 30 倍）", wa)
	}
}

// TestAgainstGoMapWithCompaction 是对拍测试的最终形态：
// 开着 compaction 跑大量随机操作，穿插重启，全程与 map 比对。
func TestAgainstGoMapWithCompaction(t *testing.T) {
	dir := t.TempDir()
	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260813))

	const (
		rounds      = 5
		opsPerRound = 6000
		keySpan     = 500
	)
	opts := &Options{MemTableSize: 12 << 10, L0CompactionTrigger: 3}

	for round := 0; round < rounds; round++ {
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("第 %d 轮打开失败: %v", round, err)
		}

		// 开头全量校验
		checkScanMatches(t, db, golden, round)

		for i := 0; i < opsPerRound; i++ {
			key := fmt.Sprintf("key%04d", rng.Intn(keySpan))
			switch rng.Intn(10) {
			case 0, 1, 2, 3, 4, 5:
				val := fmt.Sprintf("r%d-i%d", round, i)
				if err := db.Put([]byte(key), []byte(val)); err != nil {
					t.Fatal(err)
				}
				golden[key] = val
			case 6, 7:
				if err := db.Delete([]byte(key)); err != nil {
					t.Fatal(err)
				}
				delete(golden, key)
			case 8:
				if rng.Intn(500) == 0 {
					if err := db.CompactAll(); err != nil {
						t.Fatal(err)
					}
				}
			default:
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

		checkScanMatches(t, db, golden, round)
		if round%2 == 0 {
			db.Close()
		}
	}

	db, _ := Open(dir, opts)
	defer db.Close()
	checkScanMatches(t, db, golden, -1)

	st := db.Stats()
	var files int
	for _, l := range st.Levels {
		files += l.NumFiles
	}
	t.Logf("%d 轮之后：%d 个存活 key，%d 个文件共 %s",
		rounds, len(golden), files, humanBytes(totalSize(st)))
	t.Logf("\n%s", st)
}

func BenchmarkPutWithCompaction(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 1 << 20})
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
	st := db.Stats()
	b.ReportMetric(st.WriteAmplification(), "写放大x")
}
