// Package bloom 实现布隆过滤器。
//
// 对应 LSM 原理的：第 9 步（快速判断 key 不存在）
//
// # 它解决什么问题
//
// 分层之后，一次 Get 仍然要问好几个地方：MemTable、L0 的每个文件、
// L1~L6 各一个文件。每问一个文件就可能真读一次磁盘 ——
// 而绝大多数时候那个文件里【压根没有】这个 key。
//
// 我们想要一个能在【不读磁盘】的前提下回答"有没有"的东西，
// 而且必须非常省内存（几百个文件，每个都要常驻一份）。
//
// # 为什么不能直接存 key
//
// 把文件里所有 key 塞进内存的 HashSet？算笔账：
// 一个 64MB 的文件约 100 万个 key、平均 20 字节，光 key 就要 20MB。
// 几百个文件就是几个 GB —— 只为了回答一个"在不在"。
//
// 问题出在：我们把【完整的 key】存下来了，
// 但其实只需要回答 1 bit 的信息。
//
// # 只存痕迹
//
// 准备一个位数组，来一个 key 就用哈希算出几个位置、置 1：
//
//	"u1" ──┬─ hash1 → 3   ──┐
//	       ├─ hash2 → 7    ├──▶ 把第 3、7、11 位置成 1
//	       └─ hash3 → 11  ──┘
//
//	下标  0  1  2  3  4  5  6  7  8  9 10 11
//	      0  0  0 [1] 0  0  0 [1] 0  0  0 [1]
//
// 查的时候算同样几个位置：
//
//	有任何一位是 0  →  这个 key 【肯定没存过】，100% 确定
//	全都是 1        →  【可能存过】，也可能是别的 key 恰好把这几位填满了
//
// 这个"单向出错"的性质正好和需求严丝合缝：
// 说"没有"就跳过文件（安全），说"有"就老实读一次（最坏白读一次，结果仍正确）。
// 过滤器只影响【性能】，永远不影响【正确性】。
//
// # 一个致命限制：不能删除
//
// 位是被多个 key 共享的，清掉某个 key 的位会连累别人 ——
// 那会造成【假阴性】（真实存在的被判为不存在），是致命的。
//
// 而这个限制在 LSM 里恰好不存在：SSTable 写完就永不修改，
// 过滤器也是一次性构建、只读的，从来不需要"删除单个 key"。
// 文件被 compaction 淘汰时是整个连同过滤器一起丢弃、重新构建。
// 两者天生一对。
package bloom

import (
	"encoding/binary"
	"math"
)

// Filter 是一个已经构建好的布隆过滤器。
type Filter struct {
	bits []byte
	k    uint8 // 哈希函数个数
}

// BitsPerKeyToK 根据"每个 key 分几 bit"算出最优的哈希函数个数。
//
//	k = (m/n) × ln2 ≈ (m/n) × 0.693
//
// 太少则每个 key 的"指纹"不够独特，太多则位数组被填得太满 —— 两头都会推高假阳性。
func BitsPerKeyToK(bitsPerKey int) uint8 {
	k := int(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return uint8(k)
}

// New 用一批 key 构建过滤器。
//
// bitsPerKey 决定内存与假阳性率的权衡：
//
//	 4 bit/key  →  假阳性约 14.7%
//	 8 bit/key  →  约 2.2%
//	10 bit/key  →  约 0.8%   ← 常用默认
//	16 bit/key  →  约 0.05%
//
// keys 允许重复，重复的 key 只会把同样的位再置一遍，无害。
func New(bitsPerKey int, keys [][]byte) *Filter {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	k := BitsPerKeyToK(bitsPerKey)

	bits := len(keys) * bitsPerKey
	if bits < 64 {
		// 位数组太小时假阳性率会失控，给个下限
		bits = 64
	}
	bytes := (bits + 7) / 8
	bits = bytes * 8

	f := &Filter{bits: make([]byte, bytes), k: k}
	for _, key := range keys {
		f.add(key)
	}
	return f
}

// add 把一个 key 的痕迹印进位数组。
func (f *Filter) add(key []byte) {
	bits := uint32(len(f.bits) * 8)
	h := hash(key)
	// 双哈希法：用一个哈希值派生出 k 个位置，
	// 省掉了真的算 k 次哈希的开销，效果在统计上等价。
	delta := h>>17 | h<<15
	for i := uint8(0); i < f.k; i++ {
		pos := h % bits
		f.bits[pos/8] |= 1 << (pos % 8)
		h += delta
	}
}

// MayContain 判断 key 是否【可能】存在。
//
//	返回 false —— 一定不存在，可以放心跳过
//	返回 true  —— 可能存在，需要真的去查一次
func (f *Filter) MayContain(key []byte) bool {
	if len(f.bits) == 0 {
		return true // 空过滤器不做任何过滤，保守地说"可能有"
	}
	bits := uint32(len(f.bits) * 8)
	h := hash(key)
	delta := h>>17 | h<<15
	for i := uint8(0); i < f.k; i++ {
		pos := h % bits
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false // 有一位是 0 —— 这是严密的推理，不是概率
		}
		h += delta
	}
	return true
}

// Encode 序列化成可以写进 SSTable 的字节：位数组 + 末尾一字节的 k。
//
// 把 k 也存进去，是为了将来改了 bitsPerKey 的配置之后，
// 老文件仍能用它自己那套参数被正确解读。
func (f *Filter) Encode() []byte {
	out := make([]byte, len(f.bits)+1)
	copy(out, f.bits)
	out[len(f.bits)] = f.k
	return out
}

// Decode 从字节还原一个过滤器。数据不合法时返回 nil，
// 调用方应当把它当作"没有过滤器"处理（退化为每次都真读，但结果依然正确）。
func Decode(data []byte) *Filter {
	if len(data) < 2 {
		return nil
	}
	k := data[len(data)-1]
	if k < 1 || k > 30 {
		return nil
	}
	return &Filter{bits: data[:len(data)-1], k: k}
}

// SizeBytes 返回过滤器占用的字节数。
func (f *Filter) SizeBytes() int { return len(f.bits) }

// K 返回哈希函数个数。
func (f *Filter) K() uint8 { return f.k }

// hash 是 FNV-1a 的 32 位变体。
//
// 布隆过滤器对哈希的要求只是"分布均匀"，不需要密码学强度，
// 所以选一个快的即可。
func hash(data []byte) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for _, b := range data {
		h ^= uint32(b)
		h *= prime32
	}
	// 再混一次，让低位也充分散开 —— 后面要对 bits 取模，
	// 低位分布不好会让位数组的前半段被过度使用。
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	return h
}

// EstimateFalsePositiveRate 估算假阳性率，用于测试和调参。
//
//	p ≈ (1 - e^(-k·n/m))^k
func EstimateFalsePositiveRate(bitsPerKey int) float64 {
	k := float64(BitsPerKeyToK(bitsPerKey))
	mn := float64(bitsPerKey)
	return math.Pow(1-math.Exp(-k/mn), k)
}

// AppendKeyTo 是个便利函数：把 key 追加到列表里，供 Writer 逐条收集。
func AppendKeyTo(keys [][]byte, key []byte) [][]byte {
	return append(keys, append([]byte(nil), key...))
}

var _ = binary.LittleEndian // 保留 import，将来存更多元信息时会用到
