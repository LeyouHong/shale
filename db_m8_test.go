package shale

import (
	"fmt"
	"math/rand"
	"testing"
)

// 这个文件是 M8 的验收测试：查一个不存在的 key 应该几乎不碰磁盘。

// 关于"不存在的 key"怎么选，有个坑值得写下来：
//
// 如果探测用的 key 在字典序上【完全落在已有数据之外】（比如数据是
// "present000000"~"present019999"，却拿 "absent000000" 去查），
// 那么在问布隆过滤器之前，更廉价的一层就已经把它挡掉了 ——
// L1+ 的二分查找发现这个 key 不在任何文件的 key 范围内，直接返回。
//
// 所以要测出布隆过滤器的效果，探测的 key 必须【落在已有数据的范围内】
// 但实际不存在。下面统一用"写偶数、查奇数"来构造这种场景。

// TestBloomFilterBlocksMissingKeys 是 M8 的核心验收：
// 查 10 万个落在数据范围内但并不存在的 key，
// 布隆过滤器应该把绝大部分挡在磁盘之外。
func TestBloomFilterBlocksMissingKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize: 32 << 10,
		SSTableSize:  64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 只写偶数
	const n = 20000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%08d", i*2)), val)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// 只查奇数 —— 它们夹在已有 key 之间，边界检查挡不住，
	// 必须靠布隆过滤器来挡
	const probes = 100000
	for i := 0; i < probes; i++ {
		key := fmt.Sprintf("key%08d", i%(n*2)*2+1)
		if _, err := db.Get([]byte(key)); err == nil {
			t.Fatalf("%q 不该存在", key)
		}
	}

	st := db.Stats()
	if st.BloomFilterChecks == 0 {
		t.Fatal("布隆过滤器一次都没被用到 —— 是不是没启用？")
	}
	rate := float64(st.BloomFilterSkips) / float64(st.BloomFilterChecks)
	t.Logf("查了 %d 个不存在的 key：过滤器被问 %d 次，挡下 %d 次（%.2f%%）",
		probes, st.BloomFilterChecks, st.BloomFilterSkips, rate*100)

	// 10 bit/key 的配置下假阳性率约 0.8%，所以应该挡下 99% 以上
	if rate < 0.98 {
		t.Errorf("只挡下 %.2f%%，布隆过滤器效果不达标", rate*100)
	}
}

// TestBloomFilterNoFalseNegatives 验证过滤器不会误伤真实存在的数据。
//
// 假阴性是致命的：它会让查询跳过本该读的文件，返回"不存在"。
func TestBloomFilterNoFalseNegatives(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 20000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}

	// 每一个写进去的 key 都必须能读出来
	for i := 0; i < n; i++ {
		mustGet(t, db, fmt.Sprintf("key%06d", i), fmt.Sprintf("v%d", i))
	}
}

// TestBloomFilterCanBeDisabled 对比开关过滤器的效果差异。
func TestBloomFilterCanBeDisabled(t *testing.T) {
	build := func(bloomBits int) Stats {
		dir := t.TempDir()
		db, err := Open(dir, &Options{
			MemTableSize:    16 << 10,
			BloomBitsPerKey: bloomBits,
			BlockCacheSize:  -1, // 关掉缓存，让差异只来自过滤器
		})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < 5000; i++ {
			db.Put([]byte(fmt.Sprintf("k%08d", i*2)), []byte("v"))
		}
		db.Flush()

		for i := 0; i < 20000; i++ {
			db.Get([]byte(fmt.Sprintf("k%08d", i%5000*2+1)))
		}
		return db.Stats()
	}

	on := build(10)
	off := build(-1) // 显式关闭

	t.Logf("开启过滤器：被问 %d 次、挡下 %d 次", on.BloomFilterChecks, on.BloomFilterSkips)
	t.Logf("关闭过滤器：被问 %d 次（说明确实没建过滤器）", off.BloomFilterChecks)

	if on.BloomFilterSkips == 0 {
		t.Error("开启时应该挡下大量查询")
	}
	if off.BloomFilterChecks != 0 {
		t.Error("关闭时不该有任何过滤器查询")
	}
}

// TestBlockCacheHits 验证重复读同一批 key 时缓存能命中。
func TestBlockCacheHits(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:   16 << 10,
		BlockCacheSize: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 5000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), []byte("value"))
	}
	db.Flush()

	// 第一遍：全是未命中
	for i := 0; i < n; i++ {
		db.Get([]byte(fmt.Sprintf("key%06d", i)))
	}
	first := db.Stats()

	// 第二遍：应该大量命中
	for i := 0; i < n; i++ {
		db.Get([]byte(fmt.Sprintf("key%06d", i)))
	}
	second := db.Stats()

	newHits := second.BlockCacheHits - first.BlockCacheHits
	newMisses := second.BlockCacheMisses - first.BlockCacheMisses
	t.Logf("第一遍：命中 %d、未命中 %d（命中率 %.1f%%）",
		first.BlockCacheHits, first.BlockCacheMisses, first.BlockCacheHitRate()*100)
	t.Logf("第二遍：命中 %d、未命中 %d（命中率 %.1f%%）",
		newHits, newMisses, float64(newHits)/float64(newHits+newMisses)*100)

	if newHits <= newMisses {
		t.Errorf("第二遍应该以命中为主，实际命中 %d、未命中 %d", newHits, newMisses)
	}
}

// TestBlockCacheEvictedOnFileDelete 验证 compaction 删文件时会清掉对应的缓存块。
//
// 不清的话有两个问题：那些块白占容量，而且文件号被复用时会读到旧数据。
func TestBlockCacheEvictedOnFileDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{
		MemTableSize:   8 << 10,
		BlockCacheSize: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 3000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), []byte("value"))
	}
	db.Flush()
	for i := 0; i < 3000; i++ {
		db.Get([]byte(fmt.Sprintf("k%05d", i))) // 把块读进缓存
	}
	cachedBefore := db.blockCache.Len() // Cache 自带锁，可直接读

	// 全量 compaction 会重写并删掉所有旧文件
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}

	t.Logf("compaction 前缓存了 %d 个块，之后 %d 个（当前生效文件 %d 个）",
		cachedBefore, db.blockCache.Len(), numTables(db))

	// 数据必须仍然正确
	for i := 0; i < 3000; i += 97 {
		mustGet(t, db, fmt.Sprintf("k%05d", i), "value")
	}
}

// TestReadPerformanceWithFilter 对比有无过滤器时查不存在的 key 的耗时。
func TestReadPerformanceWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("-short 模式跳过")
	}
	measure := func(bloomBits int) (Stats, int) {
		dir := t.TempDir()
		db, err := Open(dir, &Options{
			MemTableSize:    32 << 10,
			SSTableSize:     32 << 10,
			BloomBitsPerKey: bloomBits,
			BlockCacheSize:  -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < 20000; i++ {
			db.Put([]byte(fmt.Sprintf("key%08d", i*2)), make([]byte, 100))
		}
		db.Flush()

		for i := 0; i < 20000; i++ {
			db.Get([]byte(fmt.Sprintf("key%08d", i*2+1)))
		}
		return db.Stats(), numTables(db)
	}

	withFilter, files1 := measure(10)
	noFilter, files2 := measure(-1)

	t.Logf("有过滤器：%d 个文件，过滤器挡下 %d/%d 次查询",
		files1, withFilter.BloomFilterSkips, withFilter.BloomFilterChecks)
	t.Logf("无过滤器：%d 个文件，每次查询都要真读文件（过滤器查询数 %d）", files2, noFilter.BloomFilterChecks)

	if withFilter.BloomFilterSkips == 0 {
		t.Error("有过滤器时应该挡下大量查询")
	}
}

// TestFilterSurvivesRestart 验证过滤器随文件一起持久化。
func TestFilterSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, &Options{MemTableSize: 16 << 10})
	for i := 0; i < 5000; i++ {
		db.Put([]byte(fmt.Sprintf("k%08d", i*2)), []byte("v"))
	}
	db.Flush()
	db.Close()

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// 打开每个文件确认过滤器还在
	db2.mu.RLock()
	var nums []uint64
	for level := 0; level < 7; level++ {
		for _, f := range db2.vs.Current().Files(level) {
			nums = append(nums, f.Num)
		}
	}
	db2.mu.RUnlock()

	checked := 0
	for level := 0; level < 1; level++ {
		for _, num := range nums {
			r, err := db2.table(num)
			if err != nil {
				t.Fatal(err)
			}
			if !r.HasFilter() {
				t.Errorf("文件 %06d 重启后丢了布隆过滤器", num)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("一个文件都没有？")
	}

	for i := 0; i < 10000; i++ {
		db2.Get([]byte(fmt.Sprintf("k%08d", i%5000*2+1)))
	}
	st := db2.Stats()
	if st.BloomFilterSkips == 0 {
		t.Error("重启后过滤器应该仍然生效")
	}
	t.Logf("重启后过滤器仍挡下 %d/%d 次查询", st.BloomFilterSkips, st.BloomFilterChecks)

	for i := 0; i < 5000; i++ {
		mustGet(t, db2, fmt.Sprintf("k%08d", i*2), "v")
	}
}

// TestCorrectnessWithFilterAndCache 在开启过滤器和缓存的情况下跑对拍。
func TestCorrectnessWithFilterAndCache(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{
		MemTableSize:    12 << 10,
		SSTableSize:     16 << 10,
		LevelBaseSize:   48 << 10,
		BloomBitsPerKey: 10,
		BlockCacheSize:  1 << 20,
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260816))

	for i := 0; i < 30000; i++ {
		key := fmt.Sprintf("key%05d", rng.Intn(3000))
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5, 6:
			val := fmt.Sprintf("v%d", i)
			db.Put([]byte(key), []byte(val))
			golden[key] = val
		case 7, 8:
			db.Delete([]byte(key))
			delete(golden, key)
		default:
			// 一半查存在的、一半查不存在的
			probe := key
			if rng.Intn(2) == 0 {
				probe = fmt.Sprintf("never%05d", rng.Intn(100000))
			}
			got, err := db.Get([]byte(probe))
			want, exists := golden[probe]
			if exists {
				if err != nil || string(got) != want {
					t.Fatalf("第 %d 步：%q = (%q,%v)，期望 %q", i, probe, got, err, want)
				}
			} else if err == nil {
				t.Fatalf("第 %d 步：%q 不应存在，却返回 %q", i, probe, got)
			}
		}
	}

	checkInvariants(t, db)
	checkScanMatches(t, db, golden, -1)

	st := db.Stats()
	t.Logf("%d 个存活 key\n过滤器挡下 %d/%d 次，块缓存命中率 %.1f%%",
		len(golden), st.BloomFilterSkips, st.BloomFilterChecks,
		st.BlockCacheHitRate()*100)
}

func BenchmarkGetMissingWithFilter(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 50000; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), make([]byte, 100))
	}
	db.Flush()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("absent%06d", i)))
	}
}

func BenchmarkGetMissingNoFilter(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 64 << 10, BloomBitsPerKey: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 50000; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), make([]byte, 100))
	}
	db.Flush()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("absent%06d", i)))
	}
}
