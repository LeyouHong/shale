package iterator

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/leyouhong/shale/internal/ikey"
)

// src 是测试用的记录描述。
type src struct {
	key  string
	seq  uint64
	kind ikey.Kind
	val  string
}

// makeIter 把一批记录做成一个源（会先按内部 key 排好序）。
func makeIter(records ...src) *SliceIterator {
	entries := make([]Entry, 0, len(records))
	for _, r := range records {
		entries = append(entries, Entry{
			Key:   ikey.Encode(nil, []byte(r.key), r.seq, r.kind),
			Value: []byte(r.val),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return ikey.Compare(entries[i].Key, entries[j].Key) < 0
	})
	return NewSliceIterator(entries)
}

// collect 把迭代器的输出收集成可读形式，如 "a#3:SET=v3"。
func collect(t *testing.T, it Iterator) []string {
	t.Helper()
	var out []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out = append(out, fmt.Sprintf("%s=%s", ikey.Debug(it.Key()), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("遍历出错: %v", err)
	}
	return out
}

func assertSeq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("输出 %d 条，期望 %d 条\n实际: %v\n期望: %v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条 = %q，期望 %q", i, got[i], want[i])
		}
	}
}

func TestSliceIterator(t *testing.T) {
	it := makeIter(
		src{"b", 2, ikey.KindSet, "vb"},
		src{"a", 1, ikey.KindSet, "va"},
		src{"c", 3, ikey.KindSet, "vc"},
	)
	assertSeq(t, collect(t, it), []string{"a#1:SET=va", "b#2:SET=vb", "c#3:SET=vc"})
}

func TestSliceIteratorSeek(t *testing.T) {
	it := makeIter(
		src{"b", 1, ikey.KindSet, "1"},
		src{"d", 2, ikey.KindSet, "2"},
		src{"f", 3, ikey.KindSet, "3"},
	)
	cases := []struct{ seek, want string }{
		{"a", "b"}, {"b", "b"}, {"c", "d"}, {"f", "f"}, {"g", ""},
	}
	for _, c := range cases {
		it.Seek(ikey.MakeSeekKey(nil, []byte(c.seek), ikey.MaxSeq))
		if c.want == "" {
			if it.Valid() {
				t.Errorf("Seek(%q) 应无效，却停在 %s", c.seek, ikey.Debug(it.Key()))
			}
			continue
		}
		if !it.Valid() || string(ikey.UserKey(it.Key())) != c.want {
			t.Errorf("Seek(%q) 应停在 %q", c.seek, c.want)
		}
	}
}

func TestMergeEmpty(t *testing.T) {
	m := NewMergingIterator()
	m.SeekToFirst()
	if m.Valid() {
		t.Error("没有源时不该有效")
	}

	m2 := NewMergingIterator(makeIter(), makeIter())
	m2.SeekToFirst()
	if m2.Valid() {
		t.Error("所有源都为空时不该有效")
	}
}

func TestMergeNilSourcesIgnored(t *testing.T) {
	m := NewMergingIterator(nil, makeIter(src{"a", 1, ikey.KindSet, "v"}), nil)
	assertSeq(t, collect(t, m), []string{"a#1:SET=v"})
}

// TestMergeInterleaved 是归并的核心场景：多个源的 key 交错。
func TestMergeInterleaved(t *testing.T) {
	m := NewMergingIterator(
		makeIter(src{"a", 1, ikey.KindSet, "1"}, src{"c", 3, ikey.KindSet, "3"}),
		makeIter(src{"b", 2, ikey.KindSet, "2"}, src{"d", 4, ikey.KindSet, "4"}),
	)
	assertSeq(t, collect(t, m), []string{
		"a#1:SET=1", "b#2:SET=2", "c#3:SET=3", "d#4:SET=4",
	})
}

// TestMergeSameKeyNewestFirst 验证归并输出里【同一个 key 的最新版本排在前面】。
//
// 这是上层去重逻辑的基石：拿到第一个就是答案，后面同 key 的全跳过。
// 它不是归并器特意做的，而是内部 key「seq 降序」排序规则的自然结果。
func TestMergeSameKeyNewestFirst(t *testing.T) {
	m := NewMergingIterator(
		makeIter(src{"k", 10, ikey.KindSet, "old"}), // 老文件
		makeIter(src{"k", 30, ikey.KindSet, "new"}), // 新文件
		makeIter(src{"k", 20, ikey.KindDelete, ""}), // 中间的墓碑
	)
	assertSeq(t, collect(t, m), []string{
		"k#30:SET=new", // 最新的先出来
		"k#20:DEL=",
		"k#10:SET=old",
	})
}

// TestMergeSourceOrderIrrelevant 验证源的传入顺序不影响结果 ——
// 新旧关系已经编码在 seq 里了，不依赖调用方怎么排列。
func TestMergeSourceOrderIrrelevant(t *testing.T) {
	a := func() *SliceIterator { return makeIter(src{"k", 10, ikey.KindSet, "old"}) }
	b := func() *SliceIterator { return makeIter(src{"k", 30, ikey.KindSet, "new"}) }

	one := collect(t, NewMergingIterator(a(), b()))
	two := collect(t, NewMergingIterator(b(), a()))
	assertSeq(t, one, two)
}

func TestMergeSeek(t *testing.T) {
	m := NewMergingIterator(
		makeIter(src{"a", 1, ikey.KindSet, "1"}, src{"e", 5, ikey.KindSet, "5"}),
		makeIter(src{"c", 3, ikey.KindSet, "3"}, src{"g", 7, ikey.KindSet, "7"}),
	)

	m.Seek(ikey.MakeSeekKey(nil, []byte("b"), ikey.MaxSeq))
	if !m.Valid() || string(ikey.UserKey(m.Key())) != "c" {
		t.Fatalf("Seek(b) 应停在 c，实际 valid=%v", m.Valid())
	}

	var got []string
	for ; m.Valid(); m.Next() {
		got = append(got, string(ikey.UserKey(m.Key())))
	}
	assertSeq(t, got, []string{"c", "e", "g"})
}

// TestMergeManySources 用大量源做压力测试，并与"全部排序"的结果对拍。
func TestMergeManySources(t *testing.T) {
	rng := rand.New(rand.NewSource(5))

	const numSources = 20
	var all []Entry
	var iters []Iterator

	for s := 0; s < numSources; s++ {
		var records []src
		n := rng.Intn(50)
		for i := 0; i < n; i++ {
			r := src{
				key:  fmt.Sprintf("k%03d", rng.Intn(200)),
				seq:  uint64(rng.Intn(10000) + 1),
				kind: ikey.KindSet,
				val:  fmt.Sprintf("s%d-i%d", s, i),
			}
			records = append(records, r)
			all = append(all, Entry{
				Key:   ikey.Encode(nil, []byte(r.key), r.seq, r.kind),
				Value: []byte(r.val),
			})
		}
		iters = append(iters, makeIter(records...))
	}

	// 标准答案：把所有记录放一起排序
	sort.Slice(all, func(i, j int) bool {
		return ikey.Compare(all[i].Key, all[j].Key) < 0
	})

	m := NewMergingIterator(iters...)
	i := 0
	for m.SeekToFirst(); m.Valid(); m.Next() {
		if i >= len(all) {
			t.Fatalf("归并输出的记录比输入的多")
		}
		if ikey.Compare(m.Key(), all[i].Key) != 0 {
			t.Fatalf("第 %d 条 = %s，期望 %s", i, ikey.Debug(m.Key()), ikey.Debug(all[i].Key))
		}
		i++
	}
	if err := m.Error(); err != nil {
		t.Fatal(err)
	}
	if i != len(all) {
		t.Fatalf("归并输出 %d 条，输入有 %d 条", i, len(all))
	}
	t.Logf("%d 个源、%d 条记录归并正确", numSources, len(all))
}

// TestMergeOutputIsSorted 验证输出严格有序 —— 归并器最基本的契约。
func TestMergeOutputIsSorted(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	var iters []Iterator
	for s := 0; s < 8; s++ {
		var records []src
		for i := 0; i < 100; i++ {
			records = append(records, src{
				key:  fmt.Sprintf("k%03d", rng.Intn(150)),
				seq:  uint64(rng.Intn(5000) + 1),
				kind: ikey.KindSet,
				val:  "v",
			})
		}
		iters = append(iters, makeIter(records...))
	}

	m := NewMergingIterator(iters...)
	var prev []byte
	n := 0
	for m.SeekToFirst(); m.Valid(); m.Next() {
		if prev != nil && ikey.Compare(prev, m.Key()) > 0 {
			t.Fatalf("第 %d 条乱序：%s 出现在 %s 之后",
				n, ikey.Debug(m.Key()), ikey.Debug(prev))
		}
		prev = append(prev[:0], m.Key()...)
		n++
	}
	if n != 800 {
		t.Errorf("输出 %d 条，期望 800 条", n)
	}
}

// errIterator 是个总是报错的源，用来验证错误能被传播出去。
type errIterator struct{ err error }

func (e *errIterator) SeekToFirst()       {}
func (e *errIterator) Seek(target []byte) {}
func (e *errIterator) Next()              {}
func (e *errIterator) Valid() bool        { return false }
func (e *errIterator) Key() []byte        { return nil }
func (e *errIterator) Value() []byte      { return nil }
func (e *errIterator) Error() error       { return e.err }
func (e *errIterator) Close() error       { return nil }

func TestMergePropagatesError(t *testing.T) {
	want := fmt.Errorf("模拟的读取失败")
	m := NewMergingIterator(
		makeIter(src{"a", 1, ikey.KindSet, "v"}),
		&errIterator{err: want},
	)
	m.SeekToFirst()
	if m.Valid() {
		t.Error("有源报错时不该继续输出")
	}
	if m.Error() != want {
		t.Errorf("错误 = %v，期望 %v", m.Error(), want)
	}
}

func BenchmarkMerge(b *testing.B) {
	const sources, perSource = 10, 1000
	build := func() []Iterator {
		var iters []Iterator
		for s := 0; s < sources; s++ {
			entries := make([]Entry, 0, perSource)
			for i := 0; i < perSource; i++ {
				entries = append(entries, Entry{
					Key:   ikey.Encode(nil, []byte(fmt.Sprintf("k%06d", i*sources+s)), uint64(i+1), ikey.KindSet),
					Value: []byte("value"),
				})
			}
			iters = append(iters, NewSliceIterator(entries))
		}
		return iters
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := NewMergingIterator(build()...)
		n := 0
		for m.SeekToFirst(); m.Valid(); m.Next() {
			n++
		}
		if n != sources*perSource {
			b.Fatalf("输出 %d 条", n)
		}
	}
}
