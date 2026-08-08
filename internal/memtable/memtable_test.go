package memtable

import (
	"fmt"
	"testing"

	"github.com/leyouhong/shale/internal/ikey"
)

// get 是个简写：用最新快照查一个 key，返回值和查找结果。
func get(m *MemTable, key string) (string, ikey.Lookup) {
	v, res := m.Get([]byte(key), ikey.MaxSeq)
	return string(v), res
}

func TestEmpty(t *testing.T) {
	m := New()
	if !m.Empty() || m.Count() != 0 {
		t.Error("新建的 MemTable 应该是空的")
	}
	if _, res := get(m, "anything"); res != ikey.NotFound {
		t.Errorf("空表查任何 key 都该是 NotFound，得到 %v", res)
	}
}

func TestAddAndGet(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("apple"), []byte("red"))
	m.Add(2, ikey.KindSet, []byte("banana"), []byte("yellow"))

	if v, res := get(m, "apple"); res != ikey.Found || v != "red" {
		t.Errorf("apple = (%q, %v)，期望 (red, Found)", v, res)
	}
	if v, res := get(m, "banana"); res != ikey.Found || v != "yellow" {
		t.Errorf("banana = (%q, %v)，期望 (yellow, Found)", v, res)
	}
	if _, res := get(m, "cherry"); res != ikey.NotFound {
		t.Errorf("cherry 应该是 NotFound，得到 %v", res)
	}
}

// TestOverwriteKeepsBothVersions 验证 LSM 的核心行为：
// 覆盖不是修改，而是【追加一个新版本】，旧版本仍然躺在里面。
func TestOverwriteKeepsBothVersions(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("k"), []byte("v1"))
	m.Add(2, ikey.KindSet, []byte("k"), []byte("v2"))
	m.Add(3, ikey.KindSet, []byte("k"), []byte("v3"))

	// 对外只看得到最新的
	if v, res := get(m, "k"); res != ikey.Found || v != "v3" {
		t.Errorf("最新值 = (%q, %v)，期望 (v3, Found)", v, res)
	}

	// 但内部确实存了 3 条记录 —— 这就是空间放大的来源
	if m.Count() != 3 {
		t.Errorf("Count = %d，期望 3（三个版本都还在）", m.Count())
	}
}

// TestSnapshotRead 验证快照读：指定 seq 就能读到"那一刻"的值。
// 这是后面实现迭代器一致性的基础。
func TestSnapshotRead(t *testing.T) {
	m := New()
	m.Add(10, ikey.KindSet, []byte("k"), []byte("v10"))
	m.Add(20, ikey.KindSet, []byte("k"), []byte("v20"))
	m.Add(30, ikey.KindSet, []byte("k"), []byte("v30"))

	cases := []struct {
		snapshot uint64
		wantVal  string
		wantRes  ikey.Lookup
	}{
		{5, "", ikey.NotFound}, // 那时这个 key 还不存在
		{10, "v10", ikey.Found},
		{15, "v10", ikey.Found}, // 落在 10 和 20 之间 → 看到 v10
		{20, "v20", ikey.Found},
		{25, "v20", ikey.Found},
		{30, "v30", ikey.Found},
		{999, "v30", ikey.Found},
	}
	for _, c := range cases {
		v, res := m.Get([]byte("k"), c.snapshot)
		if res != c.wantRes || string(v) != c.wantVal {
			t.Errorf("snapshot=%d：得到 (%q, %v)，期望 (%q, %v)",
				c.snapshot, v, res, c.wantVal, c.wantRes)
		}
	}
}

// TestDeleteWritesTombstone 验证删除写的是墓碑，
// 而且 Deleted 和 NotFound 必须能区分开 —— 这是逐层查找能正确终止的前提。
func TestDeleteWritesTombstone(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("k"), []byte("v"))
	m.Add(2, ikey.KindDelete, []byte("k"), nil)

	v, res := get(m, "k")
	if res != ikey.Deleted {
		t.Errorf("删除后应返回 Deleted（而不是 NotFound），得到 %v", res)
	}
	if v != "" {
		t.Errorf("墓碑不应带值，得到 %q", v)
	}

	// 从没写过的 key 才是 NotFound
	if _, res := get(m, "never-written"); res != ikey.NotFound {
		t.Errorf("没写过的 key 应是 NotFound，得到 %v", res)
	}

	// 墓碑也是一条记录
	if m.Count() != 2 {
		t.Errorf("Count = %d，期望 2（值 + 墓碑）", m.Count())
	}

	// 删了还能再写回来
	m.Add(3, ikey.KindSet, []byte("k"), []byte("back"))
	if v, res := get(m, "k"); res != ikey.Found || v != "back" {
		t.Errorf("重新写入后 = (%q, %v)，期望 (back, Found)", v, res)
	}
}

// TestDeleteThenSnapshotBefore 验证在删除【之前】的快照仍能读到值。
func TestDeleteThenSnapshotBefore(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("k"), []byte("v"))
	m.Add(2, ikey.KindDelete, []byte("k"), nil)

	if v, res := m.Get([]byte("k"), 1); res != ikey.Found || string(v) != "v" {
		t.Errorf("seq=1 的快照应该还看得到值，得到 (%q, %v)", v, res)
	}
	if _, res := m.Get([]byte("k"), 2); res != ikey.Deleted {
		t.Errorf("seq=2 的快照应该看到墓碑，得到 %v", res)
	}
}

func TestEmptyValueIsNotDeletion(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("k"), []byte{})

	v, res := get(m, "k")
	if res != ikey.Found {
		t.Errorf("写入空 value 后应是 Found（不是 Deleted），得到 %v", res)
	}
	if v != "" {
		t.Errorf("值应为空串，得到 %q", v)
	}
}

func TestAddCopiesInput(t *testing.T) {
	m := New()
	key := []byte("key1")
	val := []byte("val1")
	m.Add(1, ikey.KindSet, key, val)

	copy(key, "XXXX")
	copy(val, "YYYY")

	if v, res := get(m, "key1"); res != ikey.Found || v != "val1" {
		t.Errorf("调用方改了缓冲区就污染了 MemTable：得到 (%q, %v)", v, res)
	}
}

// TestIteratorSeesAllVersions 验证迭代器能看到所有版本和墓碑 ——
// flush 到 SSTable 时必须原样写出去，不能在这一步就丢弃旧版本。
func TestIteratorSeesAllVersions(t *testing.T) {
	m := New()
	m.Add(1, ikey.KindSet, []byte("a"), []byte("a1"))
	m.Add(2, ikey.KindSet, []byte("b"), []byte("b1"))
	m.Add(3, ikey.KindSet, []byte("a"), []byte("a2"))
	m.Add(4, ikey.KindDelete, []byte("b"), nil)

	type entry struct {
		key  string
		seq  uint64
		kind ikey.Kind
		val  string
	}
	var got []entry

	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		got = append(got, entry{string(it.UserKey()), it.Seq(), it.Kind(), string(it.Value())})
	}

	// 期望顺序：user key 升序，同 key 内 seq 降序
	want := []entry{
		{"a", 3, ikey.KindSet, "a2"},
		{"a", 1, ikey.KindSet, "a1"},
		{"b", 4, ikey.KindDelete, ""},
		{"b", 2, ikey.KindSet, "b1"},
	}
	if len(got) != len(want) {
		t.Fatalf("遍历出 %d 条，期望 %d 条：%+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条 = %+v，期望 %+v", i, got[i], want[i])
		}
	}
}

func TestIteratorSeekUserKey(t *testing.T) {
	m := New()
	for i, k := range []string{"a", "c", "e"} {
		m.Add(uint64(i+1), ikey.KindSet, []byte(k), []byte("v"+k))
	}

	it := m.NewIterator()
	it.SeekUserKey([]byte("b"), ikey.MaxSeq)
	if !it.Valid() || string(it.UserKey()) != "c" {
		t.Errorf("Seek(b) 应落到 c，实际 valid=%v key=%q", it.Valid(), it.UserKey())
	}

	it.SeekUserKey([]byte("z"), ikey.MaxSeq)
	if it.Valid() {
		t.Errorf("Seek(z) 应该越界无效，却停在 %q", it.UserKey())
	}
}

func TestSizeAndCount(t *testing.T) {
	m := New()
	if m.Size() != 0 {
		t.Errorf("空表 Size = %d，期望 0", m.Size())
	}

	for i := 0; i < 100; i++ {
		m.Add(uint64(i+1), ikey.KindSet,
			[]byte(fmt.Sprintf("key%03d", i)), []byte("some-value"))
	}
	if m.Count() != 100 {
		t.Errorf("Count = %d，期望 100", m.Count())
	}
	if m.Size() < 100*(6+10) {
		t.Errorf("Size = %d，明显偏小", m.Size())
	}
	t.Logf("100 条记录占用约 %d 字节（含节点开销）", m.Size())
}

func BenchmarkAdd(b *testing.B) {
	m := New()
	val := []byte("some-value-of-moderate-length")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(uint64(i+1), ikey.KindSet, []byte(fmt.Sprintf("key%013d", i)), val)
	}
}

func BenchmarkGet(b *testing.B) {
	m := New()
	const n = 100000
	for i := 0; i < n; i++ {
		m.Add(uint64(i+1), ikey.KindSet, []byte(fmt.Sprintf("key%013d", i)), []byte("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Get([]byte(fmt.Sprintf("key%013d", i%n)), ikey.MaxSeq)
	}
}
