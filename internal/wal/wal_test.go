package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

// roundTrip 把一批记录写进去再读出来，返回读到的结果。
func roundTrip(t *testing.T, records [][]byte) [][]byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for i, rec := range records {
		if err := w.Write(rec); err != nil {
			t.Fatalf("写第 %d 条失败: %v", i, err)
		}
	}

	var got [][]byte
	r := NewReader(bytes.NewReader(buf.Bytes()))
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("读第 %d 条失败: %v", len(got), err)
		}
		got = append(got, append([]byte(nil), rec...))
	}
	return got
}

func assertSame(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("读出 %d 条，期望 %d 条", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("第 %d 条长度 %d，期望长度 %d", i, len(got[i]), len(want[i]))
		}
	}
}

func TestEmptyLog(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("空日志应返回 io.EOF，得到 %v", err)
	}
	if r.ValidSize() != 0 {
		t.Errorf("ValidSize = %d，期望 0", r.ValidSize())
	}
}

func TestSmallRecords(t *testing.T) {
	want := [][]byte{
		[]byte("hello"),
		[]byte(""), // 空记录也必须支持
		[]byte("world"),
		[]byte("\x00\xff binary \x01"),
	}
	assertSame(t, roundTrip(t, want), want)
}

// TestRecordSpanningBlocks 验证跨块记录能被正确切分和拼回。
func TestRecordSpanningBlocks(t *testing.T) {
	// 造几条长度各异的大记录，必然跨块
	want := [][]byte{
		bytes.Repeat([]byte("a"), BlockSize-100), // 差一点占满一块
		bytes.Repeat([]byte("b"), BlockSize),     // 正好一块（加上头部必然跨块）
		bytes.Repeat([]byte("c"), BlockSize*2+7), // 跨三块
		[]byte("small"),
	}
	assertSame(t, roundTrip(t, want), want)
}

// TestBlockBoundaryPadding 专门覆盖「块尾放不下头部，要填充换块」这条路径。
func TestBlockBoundaryPadding(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// 先写一条，让块内只剩 3 字节（< headerSize=7）
	filler := make([]byte, BlockSize-headerSize-3)
	if err := w.Write(filler); err != nil {
		t.Fatal(err)
	}
	// 再写一条 —— 必须触发填充并换块
	second := []byte("after padding")
	if err := w.Write(second); err != nil {
		t.Fatal(err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	first, err := r.Next()
	if err != nil || len(first) != len(filler) {
		t.Fatalf("第一条读取异常: len=%d err=%v", len(first), err)
	}
	got, err := r.Next()
	if err != nil {
		t.Fatalf("第二条读取失败: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Errorf("第二条 = %q，期望 %q", got, second)
	}
}

func TestManyRandomRecords(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	var want [][]byte
	for i := 0; i < 2000; i++ {
		n := rng.Intn(200)
		if i%50 == 0 {
			n = rng.Intn(BlockSize * 2) // 偶尔来条大的，制造跨块
		}
		rec := make([]byte, n)
		rng.Read(rec)
		want = append(want, rec)
	}
	assertSame(t, roundTrip(t, want), want)
}

// TestTruncatedTail 是本包最重要的测试：
// 模拟"写到一半崩溃"，验证能读出所有完整记录、把残片当作正常结束。
func TestTruncatedTail(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	records := [][]byte{
		[]byte("record-one"),
		[]byte("record-two"),
		[]byte("record-three"),
	}
	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
	}
	full := buf.Bytes()

	// 从第 1 字节开始逐字节截断，每种截断长度都要能优雅处理
	for cut := 1; cut < len(full); cut++ {
		data := full[:cut]

		var got [][]byte
		r := NewReader(bytes.NewReader(data))
		for {
			rec, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("截断到 %d 字节时出错（应视为正常截断）: %v", cut, err)
			}
			got = append(got, append([]byte(nil), rec...))
		}

		// 读出来的必须是原记录的一个前缀 —— 绝不能读出错误的内容
		if len(got) > len(records) {
			t.Fatalf("截断到 %d 字节却读出 %d 条（原本只有 %d 条）",
				cut, len(got), len(records))
		}
		for i := range got {
			if !bytes.Equal(got[i], records[i]) {
				t.Fatalf("截断到 %d 字节：第 %d 条 = %q，期望 %q",
					cut, i, got[i], records[i])
			}
		}

		// ValidSize 必须落在已读记录的边界上，据此截断文件才安全
		if r.ValidSize() > int64(cut) {
			t.Fatalf("截断到 %d 字节，ValidSize=%d 却超过了文件长度", cut, r.ValidSize())
		}
	}
}

// TestValidSizeAllowsResume 验证按 ValidSize 截断后能安全地继续追加。
// 这正是崩溃恢复的核心流程。
func TestValidSizeAllowsResume(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, s := range []string{"first", "second"} {
		if err := w.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟崩溃：在末尾留下 5 字节垃圾
	broken := append(append([]byte(nil), buf.Bytes()...), 0xDE, 0xAD, 0xBE, 0xEF, 0x00)

	// 恢复：读到能读的为止，拿到 ValidSize
	r := NewReader(bytes.NewReader(broken))
	var recovered []string
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("恢复时出错: %v", err)
		}
		recovered = append(recovered, string(rec))
	}
	if len(recovered) != 2 || recovered[0] != "first" || recovered[1] != "second" {
		t.Fatalf("恢复出 %v，期望 [first second]", recovered)
	}

	// 按 ValidSize 截断，从那里继续写
	truncated := broken[:r.ValidSize()]
	var buf2 bytes.Buffer
	buf2.Write(truncated)
	w2 := NewWriterAt(&buf2, int64(len(truncated)))
	if err := w2.Write([]byte("third")); err != nil {
		t.Fatal(err)
	}

	// 现在应该能完整读出三条
	r2 := NewReader(bytes.NewReader(buf2.Bytes()))
	var final []string
	for {
		rec, err := r2.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("续写后读取失败: %v", err)
		}
		final = append(final, string(rec))
	}
	want := []string{"first", "second", "third"}
	if len(final) != 3 {
		t.Fatalf("续写后读出 %v，期望 %v", final, want)
	}
	for i := range want {
		if final[i] != want[i] {
			t.Errorf("第 %d 条 = %q，期望 %q", i, final[i], want[i])
		}
	}
}

// TestResumeAcrossBlockBoundary 验证从块中间的偏移续写也能正确分块。
func TestResumeAcrossBlockBoundary(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	// 写到接近块尾
	if err := w.Write(bytes.Repeat([]byte("x"), BlockSize-headerSize-20)); err != nil {
		t.Fatal(err)
	}
	size := int64(buf.Len())

	// 从这个偏移续写
	w2 := NewWriterAt(&buf, size)
	big := bytes.Repeat([]byte("y"), 5000) // 必然跨过块边界
	if err := w2.Write(big); err != nil {
		t.Fatal(err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	r.Next() // 跳过第一条
	got, err := r.Next()
	if err != nil {
		t.Fatalf("续写的记录读不出来: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Errorf("续写的记录内容不对：长度 %d，期望 %d", len(got), len(big))
	}
}

// TestCorruptPayloadDetected 验证内容被篡改能被 CRC 抓住 ——
// 这和「尾部截断」是两回事，必须报错而不是静默返回坏数据。
func TestCorruptPayloadDetected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Write([]byte("important-data"))
	w.Write([]byte("second-record"))

	data := append([]byte(nil), buf.Bytes()...)
	// 篡改第一条记录的内容（跳过 7 字节头部）
	data[headerSize+2] ^= 0xFF

	r := NewReader(bytes.NewReader(data))
	_, err := r.Next()
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("篡改内容应返回 ErrCorrupt，得到 %v", err)
	}
}

func TestCorruptTypeDetected(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Write([]byte("data"))

	data := append([]byte(nil), buf.Bytes()...)
	data[6] = byte(typeMiddle) // 把 FULL 改成 MIDDLE

	r := NewReader(bytes.NewReader(data))
	if _, err := r.Next(); !errors.Is(err, ErrCorrupt) {
		t.Errorf("篡改类型应返回 ErrCorrupt，得到 %v", err)
	}
}

func TestWriterSize(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Write([]byte("abc"))
	if w.Size() != int64(buf.Len()) {
		t.Errorf("Size = %d，实际写了 %d 字节", w.Size(), buf.Len())
	}
	if w.Size() != headerSize+3 {
		t.Errorf("Size = %d，期望 %d", w.Size(), headerSize+3)
	}
}

func BenchmarkWrite(b *testing.B) {
	var buf bytes.Buffer
	buf.Grow(b.N * 128)
	w := NewWriter(&buf)
	rec := bytes.Repeat([]byte("x"), 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	rec := bytes.Repeat([]byte("x"), 100)
	const n = 10000
	for i := 0; i < n; i++ {
		w.Write(rec)
	}
	data := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewReader(bytes.NewReader(data))
		count := 0
		for {
			if _, err := r.Next(); err != nil {
				break
			}
			count++
		}
		if count != n {
			b.Fatalf("读出 %d 条，期望 %d", count, n)
		}
	}
}

func ExampleWriter() {
	var buf bytes.Buffer

	w := NewWriter(&buf)
	w.Write([]byte("first"))
	w.Write([]byte("second"))

	r := NewReader(bytes.NewReader(buf.Bytes()))
	for {
		rec, err := r.Next()
		if err != nil {
			break
		}
		fmt.Println(string(rec))
	}
	// Output:
	// first
	// second
}
