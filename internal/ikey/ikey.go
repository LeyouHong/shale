// Package ikey 定义 shale 的【内部 key】编码。
//
// 这是整个引擎最核心、也最难改的一个决策 —— 所有其他模块
// （跳表、MemTable、WAL、SSTable、compaction）存的都是内部 key，
// 一旦格式定错，全盘返工。
//
// # 为什么需要内部 key
//
// LSM 从不原地修改数据：改一个 key 就追加一个新版本，删一个 key 就追加一个墓碑。
// 于是同一个用户 key 会有多个版本散落在 MemTable 和各层 SSTable 里，
// 必须有办法区分它们的新旧、以及区分"写入"和"删除"。
//
// 内部 key 就是在用户 key 后面挂一个 8 字节的尾巴来做这件事：
//
//	┌──────────────────┬──────────────────────────────┐
//	│   user key       │  trailer (8 字节)             │
//	│   任意长度        │  = seq(7字节) << 8 | kind(1字节) │
//	└──────────────────┴──────────────────────────────┘
//
//	seq  : 全局单调递增的序号，每次写入 +1。数字越大越新。
//	kind : 0 = 墓碑(删除)，1 = 正常写入。
//
// # 排序规则（关键）
//
//	先按 user key 【升序】
//	user key 相同时按 seq 【降序】—— 也就是【新版本排在前面】
//
// 为什么 seq 要降序？因为这样查找时定位到第一个匹配 user key 的项，
// 它天然就是最新版本，直接返回即可，不用继续往后扫。
//
// 举例：key="a" 先后写了三次（seq=5 写入、seq=9 写入、seq=12 删除），
// 排序结果是：
//
//	("a", 12, 墓碑)   ← Get 定位到这条 → 返回 NotFound ✓
//	("a",  9, 写入)
//	("a",  5, 写入)
//
// # 为什么不用 bytes.Compare 直接比
//
// 因为 seq 需要【降序】，而字节序天然是升序。
// 可以把 trailer 按位取反来骗过字节序，但那样调试时看到的字节是反的、很难读。
// 这里选择显式提供 Compare 函数（和 LevelDB 的 InternalKeyComparator 一致），
// 清晰优先。所有需要排序的地方都必须用本包的 Compare，不能用 bytes.Compare。
package ikey

import (
	"encoding/binary"
	"fmt"
)

// Kind 标记这条记录是"写入"还是"删除"。
type Kind uint8

const (
	// KindDelete 是墓碑：表示这个 key 在此刻被删除了。
	// 它本身也是一条要写进磁盘的记录，只有 compaction 推到最底层时才能真正丢弃。
	KindDelete Kind = 0

	// KindSet 是正常写入。
	KindSet Kind = 1

	// KindSeek 用于构造查找用的内部 key。
	//
	// 查找时我们想要"seq <= 目标值的所有版本里最新的那个"。
	// 由于同一个 user key 内部按 seq 降序排，构造一个
	// (userKey, seq, KindSeek) 去做 lower bound 查找即可。
	// 取值必须是【最大的合法 kind】，这样在 seq 相同时它排在最前面。
	KindSeek = KindSet
)

func (k Kind) String() string {
	switch k {
	case KindDelete:
		return "DEL"
	case KindSet:
		return "SET"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

const (
	// TrailerSize 是内部 key 尾部的固定长度。
	TrailerSize = 8

	// MaxSeq 是序号的最大值。seq 只占 7 字节，所以上限是 2^56-1。
	// 这个数足够大：即使每秒写 1000 万次，也能用两亿年。
	MaxSeq = uint64(1)<<56 - 1
)

// Encode 把 (userKey, seq, kind) 编码成内部 key，追加到 dst 后面并返回。
//
// dst 可以传 nil；传一个有足够容量的切片可以避免内存分配，例如：
//
//	buf = ikey.Encode(buf[:0], key, seq, ikey.KindSet)
func Encode(dst, userKey []byte, seq uint64, kind Kind) []byte {
	if seq > MaxSeq {
		panic(fmt.Sprintf("ikey: seq %d 超出上限 %d", seq, MaxSeq))
	}
	dst = append(dst, userKey...)
	var trailer [TrailerSize]byte
	binary.LittleEndian.PutUint64(trailer[:], packTrailer(seq, kind))
	return append(dst, trailer[:]...)
}

// UserKey 从内部 key 里取出用户 key 部分。
// 返回的是原切片的子切片，不做拷贝 —— 调用方不要修改它。
func UserKey(ik []byte) []byte {
	if len(ik) < TrailerSize {
		panic("ikey: 内部 key 长度不足")
	}
	return ik[:len(ik)-TrailerSize]
}

// Seq 取出序号。
func Seq(ik []byte) uint64 {
	return unpackTrailer(ik) >> 8
}

// GetKind 取出记录类型。
func GetKind(ik []byte) Kind {
	return Kind(unpackTrailer(ik) & 0xff)
}

// Split 一次性拆出三个部分，比分别调用省事。
func Split(ik []byte) (userKey []byte, seq uint64, kind Kind) {
	t := unpackTrailer(ik)
	return ik[:len(ik)-TrailerSize], t >> 8, Kind(t & 0xff)
}

// Valid 检查一个内部 key 是否格式合法。
// 从磁盘读上来的数据必须先过这一关，防止损坏数据引发 panic。
func Valid(ik []byte) bool {
	if len(ik) < TrailerSize {
		return false
	}
	switch GetKind(ik) {
	case KindDelete, KindSet:
		return true
	default:
		return false
	}
}

// Compare 比较两个内部 key。
//
//	先比 user key（字节序升序）
//	user key 相同时，比 seq（【降序】—— 新的排前面）
//	seq 也相同时，比 kind（降序，仅为了给出全序，实际上 seq 是唯一的）
//
// 返回值语义同 bytes.Compare：a<b 返回 -1，a==b 返回 0，a>b 返回 1。
//
// 注意：跳表、SSTable、归并迭代器等所有需要排序的地方，
// 都必须用这个函数，不能用 bytes.Compare。
func Compare(a, b []byte) int {
	if c := compareBytes(UserKey(a), UserKey(b)); c != 0 {
		return c
	}
	// user key 相同 —— 比 trailer，但方向【反过来】：
	// trailer 大（seq 新）的排在前面。
	ta, tb := unpackTrailer(a), unpackTrailer(b)
	switch {
	case ta > tb:
		return -1 // a 更新，排前面
	case ta < tb:
		return 1
	default:
		return 0
	}
}

// CompareUserKey 只比较用户 key 部分，忽略 seq 和 kind。
// 用于判断"这两条内部 key 是不是同一个用户 key 的不同版本"。
func CompareUserKey(a, b []byte) int {
	return compareBytes(UserKey(a), UserKey(b))
}

// MakeSeekKey 构造一个用于查找的内部 key。
//
// 语义："我想找 userKey 在 seq 时刻及之前的最新版本"。
// 由于同一个 user key 按 seq 降序排列，用这个 key 做
// "第一个 >= 它的位置"查找，落点就是答案。
func MakeSeekKey(dst, userKey []byte, seq uint64) []byte {
	return Encode(dst, userKey, seq, KindSeek)
}

// Debug 返回人类可读的形式，形如 `foo#12:SET`，仅用于日志和测试。
func Debug(ik []byte) string {
	if !Valid(ik) {
		return fmt.Sprintf("<invalid ikey %q>", ik)
	}
	uk, seq, kind := Split(ik)
	return fmt.Sprintf("%s#%d:%s", uk, seq, kind)
}

// ── 内部辅助 ────────────────────────────────────────────────

func packTrailer(seq uint64, kind Kind) uint64 {
	return seq<<8 | uint64(kind)
}

func unpackTrailer(ik []byte) uint64 {
	if len(ik) < TrailerSize {
		panic("ikey: 内部 key 长度不足")
	}
	return binary.LittleEndian.Uint64(ik[len(ik)-TrailerSize:])
}

// compareBytes 是 bytes.Compare 的等价实现。
// 单独写出来是为了强调：user key 之间用的就是普通字节序，
// 特殊的只有 trailer 那一段。
func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
