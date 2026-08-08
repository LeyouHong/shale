package ikey

import (
	"math/rand"
	"sort"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	cases := []struct {
		userKey string
		seq     uint64
		kind    Kind
	}{
		{"", 0, KindSet},             // 空 key 是合法的
		{"a", 1, KindSet},            //
		{"hello", 12345, KindDelete}, //
		{"foo", MaxSeq, KindSet},     // 边界：最大 seq
		{"\x00\xff", 7, KindDelete},  // key 里含任意字节
	}

	for _, c := range cases {
		ik := Encode(nil, []byte(c.userKey), c.seq, c.kind)

		if !Valid(ik) {
			t.Fatalf("Encode 出来的 key 居然非法: %s", Debug(ik))
		}
		if got := string(UserKey(ik)); got != c.userKey {
			t.Errorf("UserKey = %q, 期望 %q", got, c.userKey)
		}
		if got := Seq(ik); got != c.seq {
			t.Errorf("Seq = %d, 期望 %d", got, c.seq)
		}
		if got := GetKind(ik); got != c.kind {
			t.Errorf("Kind = %v, 期望 %v", got, c.kind)
		}
		if len(ik) != len(c.userKey)+TrailerSize {
			t.Errorf("长度 = %d, 期望 %d", len(ik), len(c.userKey)+TrailerSize)
		}

		// Split 应该和逐个取值一致
		uk, seq, kind := Split(ik)
		if string(uk) != c.userKey || seq != c.seq || kind != c.kind {
			t.Errorf("Split 结果不一致: %q %d %v", uk, seq, kind)
		}
	}
}

// TestEncodeReusesBuffer 验证 Encode 能复用调用方的缓冲区（避免每次分配）。
func TestEncodeReusesBuffer(t *testing.T) {
	buf := make([]byte, 0, 64)
	for i := 0; i < 100; i++ {
		buf = Encode(buf[:0], []byte("somekey"), uint64(i), KindSet)
		if Seq(buf) != uint64(i) {
			t.Fatalf("第 %d 次复用后 seq 错了", i)
		}
	}
	if cap(buf) != 64 {
		t.Errorf("缓冲区被重新分配了，cap = %d，期望仍是 64", cap(buf))
	}
}

// TestCompareOrder 是本包最重要的测试：
// 验证「user key 升序 + seq 降序」这条排序规则。
func TestCompareOrder(t *testing.T) {
	// 故意打乱顺序放进来
	keys := [][]byte{
		Encode(nil, []byte("b"), 1, KindSet),
		Encode(nil, []byte("a"), 5, KindSet),
		Encode(nil, []byte("a"), 12, KindDelete),
		Encode(nil, []byte("c"), 3, KindSet),
		Encode(nil, []byte("a"), 9, KindSet),
		Encode(nil, []byte("b"), 20, KindDelete),
	}

	sort.Slice(keys, func(i, j int) bool { return Compare(keys[i], keys[j]) < 0 })

	want := []string{
		"a#12:DEL", // a 的三个版本：seq 大的在前
		"a#9:SET",
		"a#5:SET",
		"b#20:DEL", // b 的两个版本
		"b#1:SET",
		"c#3:SET",
	}
	for i, w := range want {
		if got := Debug(keys[i]); got != w {
			t.Errorf("第 %d 位 = %s, 期望 %s", i, got, w)
		}
	}
}

// TestCompareIsTotalOrder 随机验证 Compare 构成一个全序关系：
// 反对称性、传递性、自反性。排序算法依赖这些性质，错了会出诡异 bug。
func TestCompareIsTotalOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	randKey := func() []byte {
		n := rng.Intn(4)
		uk := make([]byte, n)
		for i := range uk {
			uk[i] = byte('a' + rng.Intn(3)) // 只用 a/b/c，制造大量重复
		}
		kind := KindSet
		if rng.Intn(2) == 0 {
			kind = KindDelete
		}
		return Encode(nil, uk, uint64(rng.Intn(5)), kind)
	}

	keys := make([][]byte, 200)
	for i := range keys {
		keys[i] = randKey()
	}

	for _, a := range keys {
		if Compare(a, a) != 0 {
			t.Fatalf("自反性被破坏: %s 和自己比不等于 0", Debug(a))
		}
		for _, b := range keys {
			ab, ba := Compare(a, b), Compare(b, a)
			if ab != -ba {
				t.Fatalf("反对称性被破坏: Compare(%s,%s)=%d 但 Compare(%s,%s)=%d",
					Debug(a), Debug(b), ab, Debug(b), Debug(a), ba)
			}
			for _, c := range keys {
				// a<=b 且 b<=c 则必须 a<=c
				if Compare(a, b) <= 0 && Compare(b, c) <= 0 && Compare(a, c) > 0 {
					t.Fatalf("传递性被破坏: %s <= %s <= %s 但 %s > %s",
						Debug(a), Debug(b), Debug(c), Debug(a), Debug(c))
				}
			}
		}
	}
}

// TestSeekKey 验证查找 key 的定位语义：
// 用 (userKey, seq) 构造的 seek key，应该排在该 user key 所有
// seq <= 目标值的版本【之前】，而排在 seq > 目标值的版本【之后】。
func TestSeekKey(t *testing.T) {
	versions := [][]byte{
		Encode(nil, []byte("k"), 30, KindSet),
		Encode(nil, []byte("k"), 20, KindSet),
		Encode(nil, []byte("k"), 10, KindSet),
	}

	// 想读 seq=25 时刻的快照 —— 应该落在 30 之后、20 之前，
	// 于是"第一个 >= seek key 的位置"正好是 seq=20 那条。
	seek := MakeSeekKey(nil, []byte("k"), 25)

	if Compare(seek, versions[0]) <= 0 {
		t.Errorf("seek(25) 应该排在 seq=30 之后")
	}
	if Compare(seek, versions[1]) >= 0 {
		t.Errorf("seek(25) 应该排在 seq=20 之前")
	}

	idx := sort.Search(len(versions), func(i int) bool {
		return Compare(versions[i], seek) >= 0
	})
	if idx != 1 || Seq(versions[idx]) != 20 {
		t.Errorf("查找落点 = %d，期望 1（seq=20 那条）", idx)
	}
}

func TestValid(t *testing.T) {
	good := Encode(nil, []byte("x"), 1, KindSet)
	if !Valid(good) {
		t.Error("正常 key 被判为非法")
	}
	if Valid(good[:TrailerSize-1]) {
		t.Error("长度不足的 key 应判为非法")
	}
	// 把 kind 改成一个未定义的值
	bad := append([]byte(nil), good...)
	bad[len(bad)-TrailerSize] = 99
	if Valid(bad) {
		t.Error("非法 kind 应判为非法")
	}
}

func TestCompareUserKey(t *testing.T) {
	a := Encode(nil, []byte("same"), 1, KindSet)
	b := Encode(nil, []byte("same"), 999, KindDelete)
	if CompareUserKey(a, b) != 0 {
		t.Error("同一个 user key 的不同版本，CompareUserKey 应返回 0")
	}
	if Compare(a, b) == 0 {
		t.Error("但完整的 Compare 必须能区分它们")
	}
}

func BenchmarkEncode(b *testing.B) {
	buf := make([]byte, 0, 64)
	key := []byte("benchmark-key")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = Encode(buf[:0], key, uint64(i), KindSet)
	}
}

func BenchmarkCompare(b *testing.B) {
	x := Encode(nil, []byte("some-key-aaa"), 100, KindSet)
	y := Encode(nil, []byte("some-key-aab"), 200, KindSet)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Compare(x, y)
	}
}
