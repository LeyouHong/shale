package bloom

import (
	"fmt"
	"math"
	"testing"
)

func keys(n int, prefix string) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("%s%08d", prefix, i))
	}
	return out
}

// TestNoFalseNegatives 是布隆过滤器最重要的性质：
// 存进去的 key 查询时【必须】返回 true。
//
// 假阴性是致命的 —— 它会让 LSM 跳过本该读的文件，返回错误的结果。
// 假阳性只是白读一次，不影响正确性。
func TestNoFalseNegatives(t *testing.T) {
	for _, bitsPerKey := range []int{1, 4, 8, 10, 16, 32} {
		t.Run(fmt.Sprintf("%dbit", bitsPerKey), func(t *testing.T) {
			ks := keys(10000, "key")
			f := New(bitsPerKey, ks)
			for _, k := range ks {
				if !f.MayContain(k) {
					t.Fatalf("假阴性！%q 明明存过却查不到（bitsPerKey=%d）", k, bitsPerKey)
				}
			}
		})
	}
}

// TestFalsePositiveRate 验证假阳性率接近理论值。
func TestFalsePositiveRate(t *testing.T) {
	const n = 20000
	for _, bitsPerKey := range []int{4, 8, 10, 16} {
		ks := keys(n, "present")
		f := New(bitsPerKey, ks)

		// 拿一批从没存过的 key 去查
		absent := keys(n, "absent")
		fp := 0
		for _, k := range absent {
			if f.MayContain(k) {
				fp++
			}
		}
		rate := float64(fp) / float64(n)
		want := EstimateFalsePositiveRate(bitsPerKey)

		t.Logf("%2d bit/key：实测假阳性 %.2f%%，理论 %.2f%%，位数组 %d 字节（%d 个哈希）",
			bitsPerKey, rate*100, want*100, f.SizeBytes(), f.K())

		// 允许一定偏差，但不能差出数量级
		if rate > want*2.5+0.01 {
			t.Errorf("%d bit/key 的假阳性率 %.3f 明显高于理论值 %.3f", bitsPerKey, rate, want)
		}
	}
}

// TestMemoryFootprint 展示布隆过滤器省了多少内存 ——
// 这正是它存在的理由。
func TestMemoryFootprint(t *testing.T) {
	const n = 1000000
	ks := keys(n, "userkey")

	var rawBytes int
	for _, k := range ks {
		rawBytes += len(k)
	}

	f := New(10, ks)
	t.Logf("100 万个 key：直接存要 %d KB，布隆过滤器只要 %d KB（省 %.1f 倍），代价是 %.2f%% 的误判",
		rawBytes/1024, f.SizeBytes()/1024,
		float64(rawBytes)/float64(f.SizeBytes()),
		EstimateFalsePositiveRate(10)*100)

	if f.SizeBytes() > rawBytes/5 {
		t.Errorf("过滤器 %d 字节，相比原始 key 的 %d 字节没省下多少", f.SizeBytes(), rawBytes)
	}
}

func TestEmptyFilter(t *testing.T) {
	f := New(10, nil)
	// 空过滤器不该崩，查什么都可以（返回什么都不影响正确性）
	f.MayContain([]byte("anything"))

	// nil 过滤器也要安全
	var nilFilter *Filter
	_ = nilFilter
}

func TestSingleKey(t *testing.T) {
	f := New(10, [][]byte{[]byte("only")})
	if !f.MayContain([]byte("only")) {
		t.Error("唯一存进去的 key 查不到")
	}
	// 大量随机 key 里应该几乎都被挡掉
	blocked := 0
	for i := 0; i < 1000; i++ {
		if !f.MayContain([]byte(fmt.Sprintf("other%d", i))) {
			blocked++
		}
	}
	if blocked < 900 {
		t.Errorf("只存了 1 个 key，却只挡下 %d/1000 个不存在的 key", blocked)
	}
}

func TestEncodeDecode(t *testing.T) {
	ks := keys(5000, "key")
	f := New(10, ks)

	data := f.Encode()
	got := Decode(data)
	if got == nil {
		t.Fatal("Decode 返回 nil")
	}
	if got.K() != f.K() {
		t.Errorf("k = %d，期望 %d", got.K(), f.K())
	}
	if got.SizeBytes() != f.SizeBytes() {
		t.Errorf("大小 = %d，期望 %d", got.SizeBytes(), f.SizeBytes())
	}

	// 解码后的行为必须和原来完全一致
	for _, k := range ks {
		if !got.MayContain(k) {
			t.Fatalf("解码后 %q 查不到了", k)
		}
	}
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("absent%d", i))
		if f.MayContain(k) != got.MayContain(k) {
			t.Fatalf("解码前后对 %q 的判断不一致", k)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{1},        // 太短
		{1, 2, 0},  // k=0 非法
		{1, 2, 99}, // k 过大
	}
	for i, data := range cases {
		if f := Decode(data); f != nil {
			t.Errorf("第 %d 个非法输入应返回 nil，得到 %+v", i, f)
		}
	}
}

func TestBitsPerKeyToK(t *testing.T) {
	cases := map[int]uint8{
		1: 1, 4: 2, 8: 5, 10: 6, 16: 11, 32: 22,
	}
	for bits, want := range cases {
		if got := BitsPerKeyToK(bits); got != want {
			t.Errorf("BitsPerKeyToK(%d) = %d，期望 %d", bits, got, want)
		}
	}
}

// TestHashDistribution 验证哈希把位均匀铺开了。
//
// 如果哈希只用到位数组的一小段，假阳性率会远高于理论值 ——
// 而功能测试完全发现不了，只有这个测试能抓到。
func TestHashDistribution(t *testing.T) {
	f := New(10, keys(10000, "key"))

	set := 0
	for _, b := range f.bits {
		set += popcount(b)
	}
	total := f.SizeBytes() * 8
	ratio := float64(set) / float64(total)

	// 最优配置下应该约有一半的位被置 1
	t.Logf("位数组 %d 位，置 1 的有 %d 位（%.1f%%）", total, set, ratio*100)
	if ratio < 0.3 || ratio > 0.7 {
		t.Errorf("置位比例 %.2f 偏离 0.5 太多，哈希分布可能有问题", ratio)
	}

	// 前后两半的置位数不该差太多
	half := len(f.bits) / 2
	var first, second int
	for i, b := range f.bits {
		if i < half {
			first += popcount(b)
		} else {
			second += popcount(b)
		}
	}
	diff := math.Abs(float64(first-second)) / float64(first+second)
	if diff > 0.1 {
		t.Errorf("位数组前后两半的置位数相差 %.1f%%，哈希分布不均", diff*100)
	}
}

func popcount(b byte) int {
	n := 0
	for ; b != 0; b >>= 1 {
		n += int(b & 1)
	}
	return n
}

func BenchmarkNew(b *testing.B) {
	ks := keys(10000, "key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		New(10, ks)
	}
}

func BenchmarkMayContain(b *testing.B) {
	f := New(10, keys(100000, "key"))
	probe := []byte("key00050000")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain(probe)
	}
}
