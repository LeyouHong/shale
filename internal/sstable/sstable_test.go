package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/leyouhong/shale/internal/ikey"
)

// entry 是测试里用的一条记录。
type entry struct {
	key  string
	seq  uint64
	kind ikey.Kind
	val  string
}

// build 把一批记录写成 SSTable，返回文件字节。
// 记录会先按内部 key 排好序（Writer 要求严格递增）。
func build(t *testing.T, entries []entry, blockSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, blockSize)
	for _, e := range entries {
		ik := ikey.Encode(nil, []byte(e.key), e.seq, e.kind)
		if err := w.Add(ik, []byte(e.val)); err != nil {
			t.Fatalf("Add(%s) 失败: %v", ikey.Debug(ik), err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish 失败: %v", err)
	}
	return buf.Bytes()
}

func openBytes(t *testing.T, data []byte) *Reader {
	t.Helper()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("打开失败: %v", err)
	}
	return r
}

func TestEmptyTable(t *testing.T) {
	data := build(t, nil, 0)
	r := openBytes(t, data)
	defer r.Close()

	if r.EntryCount() != 0 {
		t.Errorf("空表 EntryCount = %d，期望 0", r.EntryCount())
	}
	_, res, err := r.Get([]byte("anything"), ikey.MaxSeq)
	if err != nil {
		t.Fatalf("查空表出错: %v", err)
	}
	if res != ikey.NotFound {
		t.Errorf("空表查任何 key 都该是 NotFound，得到 %v", res)
	}

	it := r.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Error("空表遍历不应有效")
	}
}

func TestWriteAndGet(t *testing.T) {
	entries := []entry{
		{"apple", 1, ikey.KindSet, "red"},
		{"banana", 2, ikey.KindSet, "yellow"},
		{"cherry", 3, ikey.KindDelete, ""},
		{"date", 4, ikey.KindSet, "brown"},
	}
	r := openBytes(t, build(t, entries, 0))
	defer r.Close()

	if r.EntryCount() != 4 {
		t.Errorf("EntryCount = %d，期望 4", r.EntryCount())
	}

	cases := []struct {
		key     string
		wantVal string
		wantRes ikey.Lookup
	}{
		{"apple", "red", ikey.Found},
		{"banana", "yellow", ikey.Found},
		{"cherry", "", ikey.Deleted}, // 墓碑
		{"date", "brown", ikey.Found},
		{"aaa", "", ikey.NotFound},       // 比最小的还小
		{"zzz", "", ikey.NotFound},       // 比最大的还大
		{"blueberry", "", ikey.NotFound}, // 落在中间的空隙
	}
	for _, c := range cases {
		v, res, err := r.Get([]byte(c.key), ikey.MaxSeq)
		if err != nil {
			t.Fatalf("Get(%q) 出错: %v", c.key, err)
		}
		if res != c.wantRes || string(v) != c.wantVal {
			t.Errorf("Get(%q) = (%q, %v)，期望 (%q, %v)",
				c.key, v, res, c.wantVal, c.wantRes)
		}
	}
}

// TestMultipleVersions 验证同一个 key 的多个版本都被原样保存，
// 且查找时按 snapshot 拿到正确的那个。
func TestMultipleVersions(t *testing.T) {
	// 内部 key 排序是「user key 升序 + seq 降序」，所以新版本在前
	entries := []entry{
		{"k", 30, ikey.KindSet, "v30"},
		{"k", 20, ikey.KindSet, "v20"},
		{"k", 10, ikey.KindSet, "v10"},
	}
	r := openBytes(t, build(t, entries, 0))
	defer r.Close()

	if r.EntryCount() != 3 {
		t.Errorf("三个版本都应保留，EntryCount = %d", r.EntryCount())
	}

	cases := []struct {
		snapshot uint64
		want     string
		res      ikey.Lookup
	}{
		{5, "", ikey.NotFound},
		{10, "v10", ikey.Found},
		{15, "v10", ikey.Found},
		{25, "v20", ikey.Found},
		{99, "v30", ikey.Found},
	}
	for _, c := range cases {
		v, res, _ := r.Get([]byte("k"), c.snapshot)
		if res != c.res || string(v) != c.want {
			t.Errorf("snapshot=%d：得到 (%q,%v)，期望 (%q,%v)",
				c.snapshot, v, res, c.want, c.res)
		}
	}
}

// TestManyBlocks 验证跨多个数据块时索引定位正确 ——
// 这是稀疏索引的核心路径。
func TestManyBlocks(t *testing.T) {
	const n = 5000
	var entries []entry
	for i := 0; i < n; i++ {
		entries = append(entries, entry{
			key:  fmt.Sprintf("key%06d", i),
			seq:  uint64(i + 1),
			kind: ikey.KindSet,
			val:  fmt.Sprintf("value-%06d-padding-to-make-it-longer", i),
		})
	}

	// 用很小的块，强制产生大量数据块
	data := build(t, entries, 512)
	r := openBytes(t, data)
	defer r.Close()

	if r.EntryCount() != n {
		t.Fatalf("EntryCount = %d，期望 %d", r.EntryCount(), n)
	}
	t.Logf("%d 条记录写成 %d 字节（块大小 512）", n, len(data))

	// 逐个查，全都要命中
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		v, res, err := r.Get([]byte(key), ikey.MaxSeq)
		if err != nil {
			t.Fatalf("Get(%q) 出错: %v", key, err)
		}
		if res != ikey.Found {
			t.Fatalf("Get(%q) = %v，期望 Found", key, res)
		}
		want := fmt.Sprintf("value-%06d-padding-to-make-it-longer", i)
		if string(v) != want {
			t.Fatalf("Get(%q) 值不对", key)
		}
	}

	// 查不存在的 key（夹在已有 key 之间）
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%06d.5", i*37)
		if _, res, _ := r.Get([]byte(key), ikey.MaxSeq); res != ikey.NotFound {
			t.Errorf("Get(%q) = %v，期望 NotFound", key, res)
		}
	}
}

func TestIteratorOrder(t *testing.T) {
	const n = 1000
	var entries []entry
	for i := 0; i < n; i++ {
		entries = append(entries, entry{
			key: fmt.Sprintf("k%04d", i), seq: uint64(i + 1),
			kind: ikey.KindSet, val: fmt.Sprintf("v%d", i),
		})
	}
	r := openBytes(t, build(t, entries, 256))
	defer r.Close()

	it := r.NewIterator()
	defer it.Close()

	count := 0
	var prev []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("第 %d 条：顺序错乱，%s 出现在 %s 之后",
				count, ikey.Debug(it.Key()), ikey.Debug(prev))
		}
		prev = append(prev[:0], it.Key()...)

		wantKey := fmt.Sprintf("k%04d", count)
		if string(ikey.UserKey(it.Key())) != wantKey {
			t.Fatalf("第 %d 条 key = %q，期望 %q", count, ikey.UserKey(it.Key()), wantKey)
		}
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("遍历出错: %v", err)
	}
	if count != n {
		t.Errorf("遍历出 %d 条，期望 %d 条", count, n)
	}
}

func TestIteratorSeek(t *testing.T) {
	entries := []entry{
		{"b", 1, ikey.KindSet, "vb"},
		{"d", 2, ikey.KindSet, "vd"},
		{"f", 3, ikey.KindSet, "vf"},
	}
	r := openBytes(t, build(t, entries, 0))
	defer r.Close()

	it := r.NewIterator()
	defer it.Close()

	cases := []struct{ seek, want string }{
		{"a", "b"},
		{"b", "b"},
		{"c", "d"},
		{"f", "f"},
		{"g", ""}, // 越界
	}
	for _, c := range cases {
		target := ikey.MakeSeekKey(nil, []byte(c.seek), ikey.MaxSeq)
		it.Seek(target)
		if c.want == "" {
			if it.Valid() {
				t.Errorf("Seek(%q) 应无效，却停在 %q", c.seek, ikey.UserKey(it.Key()))
			}
			continue
		}
		if !it.Valid() {
			t.Errorf("Seek(%q) 应停在 %q，却无效", c.seek, c.want)
			continue
		}
		if got := string(ikey.UserKey(it.Key())); got != c.want {
			t.Errorf("Seek(%q) 停在 %q，期望 %q", c.seek, got, c.want)
		}
	}
}

// TestOutOfOrderRejected 验证乱序写入会被当场拒绝 ——
// SSTable 的一切都建立在"有序"这个前提上，写错了整个文件就废了。
func TestOutOfOrderRejected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 0)

	k1 := ikey.Encode(nil, []byte("b"), 1, ikey.KindSet)
	k2 := ikey.Encode(nil, []byte("a"), 2, ikey.KindSet)

	if err := w.Add(k1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(k2, []byte("v")); err == nil {
		t.Error("乱序写入应该被拒绝")
	}
	// 重复的 key 也不行
	if err := w.Add(k1, []byte("v")); err == nil {
		t.Error("重复 key 应该被拒绝")
	}
}

func TestKeyRange(t *testing.T) {
	entries := []entry{
		{"aaa", 1, ikey.KindSet, "v"},
		{"mmm", 2, ikey.KindSet, "v"},
		{"zzz", 3, ikey.KindSet, "v"},
	}
	r := openBytes(t, build(t, entries, 0))
	defer r.Close()

	if got := string(ikey.UserKey(r.FirstKey())); got != "aaa" {
		t.Errorf("FirstKey = %q，期望 aaa", got)
	}
	if got := string(ikey.UserKey(r.LastKey())); got != "zzz" {
		t.Errorf("LastKey = %q，期望 zzz", got)
	}
}

func TestLargeValues(t *testing.T) {
	// 单条 value 远超一个块 —— 必须能正常写入和读出
	sizes := []int{100, 4 << 10, 64 << 10, 256 << 10}
	var entries []entry
	for i, size := range sizes {
		entries = append(entries, entry{
			key: fmt.Sprintf("big%d", i), seq: uint64(i + 1),
			kind: ikey.KindSet, val: string(bytes.Repeat([]byte{byte('a' + i)}, size)),
		})
	}
	r := openBytes(t, build(t, entries, 4<<10))
	defer r.Close()

	for i, size := range sizes {
		v, res, err := r.Get([]byte(fmt.Sprintf("big%d", i)), ikey.MaxSeq)
		if err != nil || res != ikey.Found {
			t.Fatalf("big%d (%d 字节): res=%v err=%v", i, size, res, err)
		}
		if len(v) != size {
			t.Errorf("big%d 长度 = %d，期望 %d", i, len(v), size)
		}
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	entries := []entry{
		{"", 1, ikey.KindSet, "value for empty key"},
		{"k", 2, ikey.KindSet, ""}, // 空值不等于删除
	}
	r := openBytes(t, build(t, entries, 0))
	defer r.Close()

	v, res, _ := r.Get([]byte(""), ikey.MaxSeq)
	if res != ikey.Found || string(v) != "value for empty key" {
		t.Errorf("空 key 查询失败: (%q, %v)", v, res)
	}
	v, res, _ = r.Get([]byte("k"), ikey.MaxSeq)
	if res != ikey.Found || len(v) != 0 {
		t.Errorf("空值查询失败: (%q, %v)，期望 Found 且值为空", v, res)
	}
}

// TestCorruptDetection 验证各种损坏都能被识别，而不是静默返回坏数据。
func TestCorruptDetection(t *testing.T) {
	entries := []entry{
		{"a", 1, ikey.KindSet, "va"},
		{"b", 2, ikey.KindSet, "vb"},
		{"c", 3, ikey.KindSet, "vc"},
	}
	good := build(t, entries, 0)

	t.Run("文件太短", func(t *testing.T) {
		if _, err := NewReader(bytes.NewReader(good[:10]), 10); err == nil {
			t.Error("应该报错")
		}
	})

	t.Run("Magic 被改坏", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[len(bad)-1] ^= 0xFF
		if _, err := NewReader(bytes.NewReader(bad), int64(len(bad))); !errors.Is(err, ErrCorrupt) {
			t.Errorf("应返回 ErrCorrupt，得到 %v", err)
		}
	})

	t.Run("数据块被篡改", func(t *testing.T) {
		bad := append([]byte(nil), good...)
		bad[5] ^= 0xFF // 改动第一个数据块的内容
		r, err := NewReader(bytes.NewReader(bad), int64(len(bad)))
		if err != nil {
			return // 打开时就发现了，也可以
		}
		defer r.Close()
		if _, _, err := r.Get([]byte("a"), ikey.MaxSeq); !errors.Is(err, ErrCorrupt) {
			t.Errorf("读被篡改的块应返回 ErrCorrupt，得到 %v", err)
		}
	})

	t.Run("尾部被截断", func(t *testing.T) {
		bad := good[:len(good)-5]
		if _, err := NewReader(bytes.NewReader(bad), int64(len(bad))); err == nil {
			t.Error("截断的文件应该打不开")
		}
	})
}

// TestOpenFile 验证从真实文件打开的路径。
func TestOpenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, 0)
	for i := 0; i < 100; i++ {
		ik := ikey.Encode(nil, []byte(fmt.Sprintf("k%03d", i)), uint64(i+1), ikey.KindSet)
		w.Add(ik, []byte("value"))
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer r.Close()

	if r.EntryCount() != 100 {
		t.Errorf("EntryCount = %d，期望 100", r.EntryCount())
	}
	if _, res, _ := r.Get([]byte("k050"), ikey.MaxSeq); res != ikey.Found {
		t.Error("k050 应该能找到")
	}
}

// TestAgainstSortedSlice 是对拍测试：
// 随机生成一批记录，SSTable 的查询结果必须与"排序切片 + 线性查找"完全一致。
func TestAgainstSortedSlice(t *testing.T) {
	rng := rand.New(rand.NewSource(3))

	// 生成随机记录，允许同一个 key 有多个版本
	type kv struct {
		ik  []byte
		val string
	}
	seen := make(map[string]bool)
	var all []kv
	for i := 0; i < 3000; i++ {
		userKey := fmt.Sprintf("k%04d", rng.Intn(800))
		seq := uint64(rng.Intn(100000) + 1)
		kind := ikey.KindSet
		if rng.Intn(5) == 0 {
			kind = ikey.KindDelete
		}
		ik := ikey.Encode(nil, []byte(userKey), seq, kind)
		if seen[string(ik)] {
			continue // Writer 要求严格递增，跳过完全重复的
		}
		seen[string(ik)] = true
		all = append(all, kv{ik, fmt.Sprintf("v%d", i)})
	}

	// 按内部 key 排序
	sortSlice(all, func(a, b kv) bool { return ikey.Compare(a.ik, b.ik) < 0 })

	var buf bytes.Buffer
	w := NewWriter(&buf, 1024)
	for _, e := range all {
		val := e.val
		if ikey.GetKind(e.ik) == ikey.KindDelete {
			val = ""
		}
		if err := w.Add(e.ik, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r := openBytes(t, buf.Bytes())
	defer r.Close()

	// 遍历必须一字不差
	it := r.NewIterator()
	defer it.Close()
	i := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if i >= len(all) {
			t.Fatalf("遍历出的记录比写入的多")
		}
		if !bytes.Equal(it.Key(), all[i].ik) {
			t.Fatalf("第 %d 条 key 不一致：%s vs %s",
				i, ikey.Debug(it.Key()), ikey.Debug(all[i].ik))
		}
		i++
	}
	if i != len(all) {
		t.Fatalf("遍历出 %d 条，写入了 %d 条", i, len(all))
	}

	// 随机点查，与线性扫描的结果比对
	for n := 0; n < 2000; n++ {
		userKey := fmt.Sprintf("k%04d", rng.Intn(900))
		snapshot := uint64(rng.Intn(100000) + 1)

		// 标准答案：在排序切片里找第一个 user key 匹配且 seq <= snapshot 的
		var wantVal string
		wantRes := ikey.NotFound
		for _, e := range all {
			if string(ikey.UserKey(e.ik)) != userKey {
				continue
			}
			if ikey.Seq(e.ik) > snapshot {
				continue
			}
			// 切片已按 seq 降序排列，第一个符合的就是最新版本
			if ikey.GetKind(e.ik) == ikey.KindDelete {
				wantRes = ikey.Deleted
			} else {
				wantRes, wantVal = ikey.Found, e.val
			}
			break
		}

		gotVal, gotRes, err := r.Get([]byte(userKey), snapshot)
		if err != nil {
			t.Fatalf("Get(%q, %d) 出错: %v", userKey, snapshot, err)
		}
		if gotRes != wantRes || (wantRes == ikey.Found && string(gotVal) != wantVal) {
			t.Fatalf("Get(%q, snapshot=%d) = (%q,%v)，期望 (%q,%v)",
				userKey, snapshot, gotVal, gotRes, wantVal, wantRes)
		}
	}
}

// sortSlice 是个小工具，避免引入 sort 包的样板代码。
func sortSlice[T any](s []T, less func(a, b T) bool) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && less(s[j], s[j-1]); j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	val := bytes.Repeat([]byte("v"), 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := NewWriter(&buf, 0)
		for j := 0; j < 1000; j++ {
			ik := ikey.Encode(nil, []byte(fmt.Sprintf("key%06d", j)), uint64(j+1), ikey.KindSet)
			w.Add(ik, val)
		}
		w.Finish()
	}
}

func BenchmarkGet(b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf, 0)
	const n = 50000
	for i := 0; i < n; i++ {
		ik := ikey.Encode(nil, []byte(fmt.Sprintf("key%06d", i)), uint64(i+1), ikey.KindSet)
		w.Add(ik, []byte("value"))
	}
	w.Finish()

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Get([]byte(fmt.Sprintf("key%06d", i%n)), ikey.MaxSeq)
	}
}
