package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func newList() *SkipList { return New(bytes.Compare) }

func TestEmpty(t *testing.T) {
	s := newList()

	if s.Len() != 0 {
		t.Errorf("空表 Len = %d，期望 0", s.Len())
	}
	if s.Contains([]byte("anything")) {
		t.Error("空表不应该包含任何 key")
	}

	it := s.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Error("空表 SeekToFirst 后不应有效")
	}
	it.SeekToLast()
	if it.Valid() {
		t.Error("空表 SeekToLast 后不应有效")
	}
	it.Seek([]byte("x"))
	if it.Valid() {
		t.Error("空表 Seek 后不应有效")
	}
}

func TestInsertAndGet(t *testing.T) {
	s := newList()
	// 故意乱序插入
	pairs := map[string]string{
		"banana": "yellow",
		"apple":  "red",
		"cherry": "dark",
		"date":   "brown",
	}
	for k, v := range pairs {
		s.Insert([]byte(k), []byte(v))
	}

	if s.Len() != len(pairs) {
		t.Errorf("Len = %d，期望 %d", s.Len(), len(pairs))
	}
	for k, want := range pairs {
		got, ok := s.Get([]byte(k))
		if !ok {
			t.Errorf("%q 应该存在", k)
			continue
		}
		if string(got) != want {
			t.Errorf("%q = %q，期望 %q", k, got, want)
		}
	}
	if _, ok := s.Get([]byte("nope")); ok {
		t.Error("不存在的 key 不应被找到")
	}
}

// TestInsertCopiesInput 验证插入时会复制 key/value，
// 这样调用方复用缓冲区不会污染已存进去的数据。
func TestInsertCopiesInput(t *testing.T) {
	s := newList()
	buf := []byte("key1")
	val := []byte("val1")
	s.Insert(buf, val)

	// 就地改掉调用方的缓冲区
	copy(buf, "XXXX")
	copy(val, "YYYY")

	got, ok := s.Get([]byte("key1"))
	if !ok {
		t.Fatal("原来的 key 找不到了 —— 说明跳表引用了调用方的切片")
	}
	if string(got) != "val1" {
		t.Errorf("value = %q，期望 val1 —— value 也被污染了", got)
	}
}

// TestIterationIsSorted 是本包的核心保证：无论以什么顺序插入，
// 遍历出来必须是有序的。
func TestIterationIsSorted(t *testing.T) {
	s := newList()
	rng := rand.New(rand.NewSource(1))

	const n = 2000
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", rng.Intn(1000000))
		if s.Contains([]byte(k)) {
			continue // 跳过重复，方便和排序后的切片比对
		}
		s.Insert([]byte(k), []byte("v"+k))
		want = append(want, k)
	}
	sort.Strings(want)

	var got []string
	it := s.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}

	if len(got) != len(want) {
		t.Fatalf("遍历出 %d 个元素，期望 %d 个", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 个 = %q，期望 %q", i, got[i], want[i])
		}
	}
}

// TestSeekSemantics 验证 Seek 的语义是「第一个 >= key」，
// 而不是「精确匹配」—— 这是 MemTable 查找的基础。
func TestSeekSemantics(t *testing.T) {
	s := newList()
	for _, k := range []string{"b", "d", "f"} {
		s.Insert([]byte(k), []byte("v"+k))
	}

	cases := []struct {
		seek string
		want string // "" 表示应该无效
	}{
		{"a", "b"}, // 比最小的还小 → 落到第一个
		{"b", "b"}, // 正好命中
		{"c", "d"}, // 落到下一个更大的
		{"d", "d"},
		{"e", "f"},
		{"f", "f"},
		{"g", ""}, // 比最大的还大 → 无效
	}
	it := s.NewIterator()
	for _, c := range cases {
		it.Seek([]byte(c.seek))
		if c.want == "" {
			if it.Valid() {
				t.Errorf("Seek(%q) 应该无效，却停在 %q", c.seek, it.Key())
			}
			continue
		}
		if !it.Valid() {
			t.Errorf("Seek(%q) 应该停在 %q，却无效了", c.seek, c.want)
			continue
		}
		if string(it.Key()) != c.want {
			t.Errorf("Seek(%q) 停在 %q，期望 %q", c.seek, it.Key(), c.want)
		}
	}
}

func TestSeekToLastAndPrev(t *testing.T) {
	s := newList()
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		s.Insert([]byte(k), []byte("v"))
	}

	it := s.NewIterator()
	it.SeekToLast()
	if !it.Valid() || string(it.Key()) != "e" {
		t.Fatalf("SeekToLast 应停在 e，实际 valid=%v", it.Valid())
	}

	// 反向走一遍
	var got []string
	for ; it.Valid(); it.Prev() {
		got = append(got, string(it.Key()))
	}
	want := []string{"e", "d", "c", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("反向遍历出 %d 个，期望 %d 个", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 个 = %q，期望 %q", i, got[i], want[i])
		}
	}
}

// TestHeightGrows 验证跳表确实长出了多层 —— 如果 randomHeight 写错
// （比如永远返回 1），功能测试仍然会全过，但性能退化成链表，
// 只有这个测试能发现。
func TestHeightGrows(t *testing.T) {
	s := newList()
	if s.Height() != 1 {
		t.Errorf("空表高度 = %d，期望 1", s.Height())
	}

	for i := 0; i < 10000; i++ {
		s.Insert([]byte(fmt.Sprintf("key%06d", i)), []byte("v"))
	}

	// 1 万个元素，每层过滤到 1/4，期望高度约 log4(10000) ≈ 6.6
	if s.Height() < 5 {
		t.Errorf("插入 1 万个元素后高度只有 %d —— 跳表退化成链表了？", s.Height())
	}
	t.Logf("1 万个元素后的跳表高度：%d", s.Height())
}

// TestHeightDistribution 验证层高分布接近 1/4 的几何分布。
func TestHeightDistribution(t *testing.T) {
	s := newList()
	const n = 100000
	counts := make(map[int]int)
	for i := 0; i < n; i++ {
		counts[s.randomHeight()]++
	}

	// 第 1 层应该约占 3/4
	ratio := float64(counts[1]) / float64(n)
	if ratio < 0.70 || ratio > 0.80 {
		t.Errorf("高度为 1 的比例 = %.3f，期望约 0.75", ratio)
	}
	// 第 2 层应该约占 3/16 ≈ 0.19
	ratio2 := float64(counts[2]) / float64(n)
	if ratio2 < 0.15 || ratio2 > 0.23 {
		t.Errorf("高度为 2 的比例 = %.3f，期望约 0.19", ratio2)
	}
	t.Logf("层高分布：1层 %.1f%%，2层 %.1f%%，3层 %.1f%%",
		ratio*100, ratio2*100, float64(counts[3])/float64(n)*100)
}

func TestSizeGrows(t *testing.T) {
	s := newList()
	if s.Size() != 0 {
		t.Errorf("空表 Size = %d，期望 0", s.Size())
	}
	s.Insert([]byte("k"), []byte("v"))
	first := s.Size()
	if first <= 0 {
		t.Fatal("插入后 Size 应大于 0")
	}
	s.Insert([]byte("k2"), bytes.Repeat([]byte("x"), 1000))
	if s.Size() <= first+1000 {
		t.Errorf("Size = %d，应该至少涨了 1000（value 大小）", s.Size())
	}
}

// TestAgainstSortedMap 是跳表的对拍测试：
// 随机插入/查询，结果必须与「map + 排序」完全一致。
func TestAgainstSortedMap(t *testing.T) {
	s := newList()
	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(7))

	const ops = 50000
	for i := 0; i < ops; i++ {
		k := fmt.Sprintf("k%04d", rng.Intn(3000)) // 制造大量重复 key
		switch rng.Intn(2) {
		case 0:
			v := fmt.Sprintf("v%d", i)
			// 跳表允许重复 key，为了能和 map 对拍，这里只插入新 key
			if _, exists := golden[k]; !exists {
				s.Insert([]byte(k), []byte(v))
				golden[k] = v
			}
		case 1:
			got, ok := s.Get([]byte(k))
			want, wantOk := golden[k]
			if ok != wantOk {
				t.Fatalf("第 %d 次操作：Get(%q) 存在性 = %v，期望 %v", i, k, ok, wantOk)
			}
			if ok && string(got) != want {
				t.Fatalf("第 %d 次操作：Get(%q) = %q，期望 %q", i, k, got, want)
			}
		}
	}

	// 最后完整遍历一次，和排序后的 map 比对
	keys := make([]string, 0, len(golden))
	for k := range golden {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	it := s.NewIterator()
	it.SeekToFirst()
	for _, k := range keys {
		if !it.Valid() {
			t.Fatalf("遍历提前结束，缺少 %q", k)
		}
		if string(it.Key()) != k {
			t.Fatalf("遍历到 %q，期望 %q", it.Key(), k)
		}
		if string(it.Value()) != golden[k] {
			t.Fatalf("%q 的值 = %q，期望 %q", k, it.Value(), golden[k])
		}
		it.Next()
	}
	if it.Valid() {
		t.Fatalf("遍历结束后还有多余元素 %q", it.Key())
	}
}

// TestCustomComparator 验证比较器确实是可注入的 ——
// MemTable 靠这一点传入 ikey.Compare（seq 降序）。
func TestCustomComparator(t *testing.T) {
	// 反序比较器
	reverse := func(a, b []byte) int { return bytes.Compare(b, a) }
	s := New(reverse)

	for _, k := range []string{"a", "b", "c"} {
		s.Insert([]byte(k), []byte("v"))
	}

	var got []string
	it := s.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"c", "b", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 个 = %q，期望 %q（反序比较器没生效）", i, got[i], want[i])
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	s := newList()
	key := make([]byte, 16)
	val := bytes.Repeat([]byte("v"), 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(key, fmt.Sprintf("key%013d", i))
		s.Insert(key, val)
	}
}

func BenchmarkGet(b *testing.B) {
	s := newList()
	const n = 100000
	for i := 0; i < n; i++ {
		s.Insert([]byte(fmt.Sprintf("key%013d", i)), []byte("value"))
	}
	rng := rand.New(rand.NewSource(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get([]byte(fmt.Sprintf("key%013d", rng.Intn(n))))
	}
}
