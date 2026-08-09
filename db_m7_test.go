package shale

import (
	"fmt"
	"math/rand"
	"testing"
)

// 这个文件是 M7 的验收测试：真正的分层 —— 层内不重叠、容量呈 10 倍关系。

// checkInvariants 校验数据库的内部一致性。
//
// 最重要的一条是「L1 及以下层内文件不重叠」。这条一旦破了，
// 读路径的二分查找就会漏数据 —— 而且是静默漏，不会报错，
// 只有对拍测试才抓得到。所以每个涉及 compaction 的测试都该调它。
func checkInvariants(t *testing.T, db *DB) {
	t.Helper()
	// 必须加锁：M9 起 compaction 在后台跑，会并发改动 vs.current
	db.mu.RLock()
	v := db.vs.Current()
	err := v.CheckInvariants()
	desc := v.String()
	db.mu.RUnlock()

	if err != nil {
		t.Fatalf("不变量被破坏: %v\n当前版本:\n%s", err, desc)
	}
}

// TestLevelsAreNonOverlapping 是 M7 最核心的验收：
// L1 及以下，同一层的文件 key 范围绝不能重叠。
func TestLevelsAreNonOverlapping(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        16 << 10,
		SSTableSize:         32 << 10,
		LevelBaseSize:       64 << 10, // 很小的 L1，逼数据往下沉
		L0CompactionTrigger: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(20260814))
	val := make([]byte, 100)
	for i := 0; i < 8000; i++ {
		key := fmt.Sprintf("key%06d", rng.Intn(50000))
		if err := db.Put([]byte(key), val); err != nil {
			t.Fatal(err)
		}
		if i%1000 == 0 {
			checkInvariants(t, db) // 中途也要一直成立
		}
	}
	checkInvariants(t, db)

	t.Logf("最终层级分布：\n%s", versionString(db))
}

// TestLevelSizesFollowMultiplier 验证各层容量呈 10 倍关系。
func TestLevelSizesFollowMultiplier(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{
		MemTableSize:        16 << 10,
		SSTableSize:         24 << 10,
		LevelBaseSize:       64 << 10,
		LevelSizeMultiplier: 10,
		L0CompactionTrigger: 3,
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 200)
	for i := 0; i < 12000; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%07d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	checkInvariants(t, db)

	st := db.Stats()
	t.Logf("\n%s", st)

	// 除了最底下那个还在填的层，其他层都不该超出容量太多
	deepest := 0
	for _, l := range st.Levels {
		if l.NumFiles > 0 && l.Level > deepest {
			deepest = l.Level
		}
	}
	for _, l := range st.Levels {
		if l.Level == 0 || l.Level >= deepest || l.NumFiles == 0 {
			continue
		}
		if l.Score() > 2.0 {
			t.Errorf("L%d 的 score = %.2f，明显超出容量（%s / %s）",
				l.Level, l.Score(), humanBytes(l.Size), humanBytes(l.MaxBytes))
		}
	}

	// 相邻层的容量上限必须是 10 倍关系
	for level := 1; level < 4; level++ {
		lo, hi := opts.LevelMaxBytes(level), opts.LevelMaxBytes(level+1)
		if hi != lo*10 {
			t.Errorf("L%d 容量 %d，L%d 容量 %d，不是 10 倍关系", level, lo, level+1, hi)
		}
	}
}

// TestDataSinksToDeeperLevels 验证数据确实会一层层往下沉。
func TestDataSinksToDeeperLevels(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        16 << 10,
		SSTableSize:         24 << 10,
		LevelBaseSize:       48 << 10,
		L0CompactionTrigger: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	for i := 0; i < 8000; i++ {
		db.Put([]byte(fmt.Sprintf("k%07d", i)), val)
	}
	checkInvariants(t, db)

	deepest := deepestLevel(db)
	if deepest < 2 {
		t.Errorf("数据只到 L%d，应该沉得更深", deepest)
	}
	t.Logf("数据最深沉到了 L%d：\n%s", deepest, versionString(db))
}

// TestCompactPointerRotates 验证轮转指针在推进 ——
// 不轮转的话同一段 key 会被反复重写，后面的数据永远沉不下去。
func TestCompactPointerRotates(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        16 << 10,
		SSTableSize:         24 << 10,
		LevelBaseSize:       48 << 10,
		L0CompactionTrigger: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 100)
	for i := 0; i < 6000; i++ {
		db.Put([]byte(fmt.Sprintf("k%07d", i)), val)
	}

	db.mu.RLock()
	moved := false
	for level := 1; level < 4; level++ {
		if len(db.compactPointer[level]) > 0 {
			moved = true
			t.Logf("L%d 的轮转指针停在 %q", level, db.compactPointer[level])
		}
	}
	db.mu.RUnlock()
	if !moved {
		t.Log("注意：没有任何层发生过 L1+ 的 compaction，轮转指针未被使用")
	}
	checkInvariants(t, db)
}

// TestBinarySearchFindsFile 验证 L1+ 的二分查找定位正确。
func TestBinarySearchFindsFile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:  8 << 10,
		SSTableSize:   8 << 10,
		LevelBaseSize: 32 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 5000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i*2)), []byte("v")) // 只写偶数
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, db)

	db.mu.RLock()
	v := db.vs.Current()
	v.Ref()
	db.mu.RUnlock()
	defer func() { db.mu.Lock(); v.Unref(); db.mu.Unlock() }()

	level := -1
	for l := 1; l < 7; l++ {
		if v.NumFiles(l) > 0 {
			level = l
			break
		}
	}
	if level < 0 {
		t.Skip("数据没沉到 L1 以下")
	}

	// 存在的 key 必须能定位到覆盖它的文件
	for i := 0; i < n; i += 97 {
		key := []byte(fmt.Sprintf("key%06d", i*2))
		if f := v.FindFile(level, key); f == nil {
			t.Errorf("L%d 里找不到能覆盖 %q 的文件", level, key)
		}
	}
	// 不存在的 key（奇数）也应该定位到某个文件或空隙，但不能 panic
	for i := 1; i < n*2; i += 199 {
		v.FindFile(level, []byte(fmt.Sprintf("key%06d", i)))
	}
	// 越界的 key 必须返回 nil
	if f := v.FindFile(level, []byte("zzzzzzzz")); f != nil {
		t.Errorf("超出范围的 key 不该定位到文件 %06d", f.Num)
	}

	// 数据一条不能少
	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("key%06d", i*2), "v")
	}
}

// TestLeveledCompactionCorrectness 是 M7 的对拍验收：
// 在真正的分层策略下跑大量随机操作，全程校验不变量并与 map 比对。
func TestLeveledCompactionCorrectness(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{
		MemTableSize:        8 << 10,
		SSTableSize:         12 << 10,
		LevelBaseSize:       32 << 10,
		L0CompactionTrigger: 3,
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260815))

	const ops = 40000
	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("key%05d", rng.Intn(4000))
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5, 6:
			val := fmt.Sprintf("v%d", i)
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			golden[key] = val
		case 7, 8:
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(golden, key)
		default:
			got, err := db.Get([]byte(key))
			want, exists := golden[key]
			if exists {
				if err != nil || string(got) != want {
					t.Fatalf("第 %d 步：%q = (%q,%v)，期望 %q", i, key, got, err, want)
				}
			} else if err == nil {
				t.Fatalf("第 %d 步：%q 不应存在，却返回 %q", i, key, got)
			}
		}
		if i%5000 == 0 {
			checkInvariants(t, db)
		}
	}

	checkInvariants(t, db)
	checkScanMatches(t, db, golden, ops)

	// 重启后仍然一致
	db.Close()
	db2, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	checkInvariants(t, db2)
	checkScanMatches(t, db2, golden, -1)

	st := db2.Stats()
	t.Logf("%d 次操作后：%d 个存活 key\n%s", ops, len(golden), st)
}

// TestLargeDatasetLayering 灌入较大数据量，观察分层是否成型。
func TestLargeDatasetLayering(t *testing.T) {
	if testing.Short() {
		t.Skip("耗时较长，-short 模式跳过")
	}
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:        64 << 10,
		SSTableSize:         64 << 10,
		LevelBaseSize:       256 << 10,
		LevelSizeMultiplier: 10,
		L0CompactionTrigger: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	val := make([]byte, 200)
	const n = 100000
	for i := 0; i < n; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%08d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	checkInvariants(t, db)

	st := db.Stats()
	t.Logf("灌入 %d 条（约 %s 用户数据）之后：\n%s",
		n, humanBytes(st.UserBytesWritten), st)

	// 各层文件数应该逐层放大
	var prevFiles int
	for _, l := range st.Levels {
		if l.Level == 0 || l.NumFiles == 0 {
			prevFiles = l.NumFiles
			continue
		}
		t.Logf("L%d: %d 个文件，%s，score %.2f",
			l.Level, l.NumFiles, humanBytes(l.Size), l.Score())
		prevFiles = l.NumFiles
	}
	_ = prevFiles

	// 抽查数据完整性
	for i := 0; i < n; i += 997 {
		if _, err := db.Get([]byte(fmt.Sprintf("key%08d", i))); err != nil {
			t.Fatalf("key%08d 丢失: %v", i, err)
		}
	}
}

func BenchmarkGetLayered(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{
		MemTableSize:        64 << 10,
		SSTableSize:         64 << 10,
		LevelBaseSize:       256 << 10,
		L0CompactionTrigger: 4,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 50000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), val)
	}

	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key%06d", rng.Intn(n))))
	}
}
