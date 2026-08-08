package shale

import (
	"bytes"
	"testing"

	"github.com/leyouhong/shale/internal/ikey"
)

// record 是遍历 Batch 时收集到的一条记录，用于断言。
type record struct {
	kind  ikey.Kind
	key   string
	value string
}

func collect(t *testing.T, b *Batch) []record {
	t.Helper()
	var got []record
	err := b.Iterate(func(kind ikey.Kind, key, value []byte) error {
		got = append(got, record{kind, string(key), string(value)})
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate 失败: %v", err)
	}
	return got
}

func TestBatchBasics(t *testing.T) {
	b := NewBatch()

	if !b.Empty() || b.Count() != 0 {
		t.Fatal("新建的 Batch 应该是空的")
	}
	if b.Size() != batchHeaderSize {
		t.Errorf("空 Batch 大小 = %d，期望 %d", b.Size(), batchHeaderSize)
	}

	b.Put([]byte("k1"), []byte("v1"))
	b.Delete([]byte("k2"))
	b.Put([]byte("k3"), []byte("")) // 空 value：合法，且不等于删除

	if b.Count() != 3 {
		t.Fatalf("Count = %d，期望 3", b.Count())
	}
	if b.Empty() {
		t.Error("有 3 条记录还说自己是空的")
	}

	want := []record{
		{ikey.KindSet, "k1", "v1"},
		{ikey.KindDelete, "k2", ""},
		{ikey.KindSet, "k3", ""},
	}
	got := collect(t, b)
	if len(got) != len(want) {
		t.Fatalf("遍历出 %d 条，期望 %d 条", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条 = %+v，期望 %+v", i, got[i], want[i])
		}
	}
}

// TestBatchPreservesOrder 验证同一个 key 的多次操作按顺序保留。
// 这很重要：Batch 内的顺序决定了它们拿到的 seq 顺序，进而决定谁覆盖谁。
func TestBatchPreservesOrder(t *testing.T) {
	b := NewBatch()
	b.Put([]byte("k"), []byte("v1"))
	b.Delete([]byte("k"))
	b.Put([]byte("k"), []byte("v2"))

	got := collect(t, b)
	want := []record{
		{ikey.KindSet, "k", "v1"},
		{ikey.KindDelete, "k", ""},
		{ikey.KindSet, "k", "v2"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条 = %+v，期望 %+v", i, got[i], want[i])
		}
	}
}

func TestBatchSeq(t *testing.T) {
	b := NewBatch()
	if b.Seq() != 0 {
		t.Error("新 Batch 的 seq 应为 0")
	}
	b.Put([]byte("k"), []byte("v"))
	b.SetSeq(12345)
	if b.Seq() != 12345 {
		t.Errorf("SetSeq 后 Seq = %d，期望 12345", b.Seq())
	}
	// 设置 seq 不应破坏记录内容
	if got := collect(t, b); len(got) != 1 || got[0].key != "k" {
		t.Error("SetSeq 破坏了记录内容")
	}
}

func TestBatchReset(t *testing.T) {
	b := NewBatch()
	b.Put([]byte("k"), []byte("v"))
	b.SetSeq(99)

	b.Reset()

	if !b.Empty() {
		t.Error("Reset 后应为空")
	}
	if b.Seq() != 0 {
		t.Errorf("Reset 后 seq = %d，期望 0", b.Seq())
	}
	if b.Size() != batchHeaderSize {
		t.Errorf("Reset 后大小 = %d，期望 %d", b.Size(), batchHeaderSize)
	}

	// 复用后应该还能正常工作
	b.Put([]byte("new"), []byte("val"))
	if got := collect(t, b); len(got) != 1 || got[0].key != "new" {
		t.Error("Reset 之后复用出问题")
	}
}

// TestBatchRoundTrip 验证「编码 → 字节 → 解码」是无损的。
// Batch 的字节形式会被直接写进 WAL，恢复时要能还原成一模一样的内容。
func TestBatchRoundTrip(t *testing.T) {
	orig := NewBatch()
	orig.Put([]byte("apple"), []byte("red"))
	orig.Delete([]byte("banana"))
	orig.Put([]byte(""), []byte("empty key is legal"))
	orig.Put([]byte("binary\x00\xff"), []byte("\x01\x02\x03"))
	orig.SetSeq(777)

	// 模拟：写进 WAL 又读回来
	onDisk := append([]byte(nil), orig.Bytes()...)

	restored := NewBatch()
	if err := restored.Load(onDisk); err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	if restored.Seq() != orig.Seq() {
		t.Errorf("seq 不一致: %d vs %d", restored.Seq(), orig.Seq())
	}
	if restored.Count() != orig.Count() {
		t.Errorf("count 不一致: %d vs %d", restored.Count(), orig.Count())
	}
	if !bytes.Equal(restored.Bytes(), orig.Bytes()) {
		t.Error("字节内容不一致")
	}

	a, bb := collect(t, orig), collect(t, restored)
	for i := range a {
		if a[i] != bb[i] {
			t.Errorf("第 %d 条不一致: %+v vs %+v", i, a[i], bb[i])
		}
	}
}

// TestBatchLoadCorrupt 验证损坏的数据能被识别出来，而不是 panic 或静默出错。
// 崩溃恢复时读到的 WAL 尾部很可能就是半条记录。
func TestBatchLoadCorrupt(t *testing.T) {
	good := NewBatch()
	good.Put([]byte("key"), []byte("value"))
	good.Delete([]byte("gone"))
	data := good.Bytes()

	t.Run("长度不足头部", func(t *testing.T) {
		b := NewBatch()
		if err := b.Load(data[:5]); err == nil {
			t.Error("应该报错")
		}
	})

	t.Run("尾部被截断", func(t *testing.T) {
		// 从 header 之后砍掉尾巴，模拟写了一半就崩了
		for cut := batchHeaderSize + 1; cut < len(data); cut++ {
			b := NewBatch()
			if err := b.Load(data[:cut]); err == nil {
				t.Errorf("截断到 %d 字节时应该报错", cut)
			}
		}
	})

	t.Run("count 虚报", func(t *testing.T) {
		bad := append([]byte(nil), data...)
		bad[8] = 99 // 把 count 改成 99
		b := NewBatch()
		if err := b.Load(bad); err == nil {
			t.Error("count 与实际记录数不符时应该报错")
		}
	})

	t.Run("kind 非法", func(t *testing.T) {
		bad := append([]byte(nil), data...)
		bad[batchHeaderSize] = 77 // 第一条记录的 kind 改成非法值
		b := NewBatch()
		if err := b.Load(bad); err == nil {
			t.Error("非法 kind 应该报错")
		}
	})

	t.Run("尾部有多余数据", func(t *testing.T) {
		bad := append(append([]byte(nil), data...), 0xAB, 0xCD)
		b := NewBatch()
		if err := b.Load(bad); err == nil {
			t.Error("尾部多余数据应该报错")
		}
	})
}

func TestBatchLargeValues(t *testing.T) {
	b := NewBatch()
	big := bytes.Repeat([]byte("x"), 100<<10) // 100KB，会用到多字节 varint
	b.Put([]byte("bigkey"), big)

	got := collect(t, b)
	if len(got) != 1 {
		t.Fatalf("期望 1 条记录，得到 %d 条", len(got))
	}
	if len(got[0].value) != len(big) {
		t.Errorf("value 长度 = %d，期望 %d", len(got[0].value), len(big))
	}
}

// TestBatchIterateStopsOnError 验证回调返回错误时立刻中止。
func TestBatchIterateStopsOnError(t *testing.T) {
	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Put([]byte("c"), []byte("3"))

	stop := errStopForTest
	count := 0
	err := b.Iterate(func(_ ikey.Kind, key, _ []byte) error {
		count++
		if string(key) == "b" {
			return stop
		}
		return nil
	})
	if err != stop {
		t.Errorf("应该原样返回回调的错误，得到 %v", err)
	}
	if count != 2 {
		t.Errorf("应该在第 2 条就停下，实际遍历了 %d 条", count)
	}
}

var errStopForTest = bytes.ErrTooLarge // 借一个现成的哨兵错误

func BenchmarkBatchPut(b *testing.B) {
	batch := NewBatch()
	key, val := []byte("some-key"), []byte("some-value-of-moderate-length")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if batch.Count() >= 1000 {
			batch.Reset()
		}
		batch.Put(key, val)
	}
}
