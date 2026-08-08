package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/leyouhong/shale/internal/ikey"
)

// Block 是 SSTable 里的最小读取单位。
//
// 文件不是一条条记录直接排下去的，而是切成一个个 4KB 左右的块：
//
//	· 读取时按块读 —— 一次 IO 拿到一批相邻的记录，顺便被缓存复用
//	· 校验也按块做 —— 每块自带 CRC，坏了能定位到具体是哪一块
//
// 块内的记录格式很朴素，一条挨一条：
//
//	┌──────────────────────────────────────────────────────┐
//	│ varint(len(ikey)) │ ikey │ varint(len(val)) │ val │…  │
//	├──────────────────────────────────────────────────────┤
//	│ CRC32 (4 字节)                                        │
//	└──────────────────────────────────────────────────────┘
//
// 块内【没有】额外的索引结构，查找靠顺序扫描。
// 这看起来很笨，但块只有 4KB、几十条记录，而且已经在内存里了 ——
// 真正的成本在"读哪一块"，那是 Index Block 的事。
//
// （LevelDB 在块内还做了前缀压缩和 restart point，本项目留到 M10 再说。）

// blockTrailerSize 是每个块尾部的 CRC32。
const blockTrailerSize = 4

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ── 构建 ────────────────────────────────────────────────────

// blockBuilder 逐条累积记录，攒够一块就吐出去。
type blockBuilder struct {
	buf     []byte
	entries int
	lastKey []byte
}

func (b *blockBuilder) reset() {
	b.buf = b.buf[:0]
	b.entries = 0
	b.lastKey = b.lastKey[:0]
}

// add 追加一条记录。调用方必须保证 key 是【递增】的。
func (b *blockBuilder) add(key, value []byte) {
	b.buf = appendLenPrefixed(b.buf, key)
	b.buf = appendLenPrefixed(b.buf, value)
	b.entries++
	b.lastKey = append(b.lastKey[:0], key...)
}

// size 返回当前累积的字节数（不含尾部 CRC）。
func (b *blockBuilder) size() int { return len(b.buf) }

func (b *blockBuilder) empty() bool { return b.entries == 0 }

// finish 封块：算出 CRC 追加到末尾，返回可以直接落盘的字节。
func (b *blockBuilder) finish() []byte {
	crc := crc32.Checksum(b.buf, crcTable)
	var tail [blockTrailerSize]byte
	binary.LittleEndian.PutUint32(tail[:], crc)
	return append(b.buf, tail[:]...)
}

// ── 解析 ────────────────────────────────────────────────────

// block 是一个已经校验过、可以遍历的数据块。
type block struct {
	data []byte // 已经剥掉尾部 CRC
}

// decodeBlock 校验并解析一个从磁盘读上来的块。
func decodeBlock(raw []byte) (block, error) {
	if len(raw) < blockTrailerSize {
		return block{}, fmt.Errorf("%w: 块长度 %d 不足", ErrCorrupt, len(raw))
	}
	body := raw[:len(raw)-blockTrailerSize]
	want := binary.LittleEndian.Uint32(raw[len(raw)-blockTrailerSize:])
	if got := crc32.Checksum(body, crcTable); got != want {
		return block{}, fmt.Errorf("%w: 块 CRC 校验失败（期望 %08x，实际 %08x）",
			ErrCorrupt, want, got)
	}
	return block{data: body}, nil
}

// blockIter 顺序遍历块内的记录。
type blockIter struct {
	data []byte
	pos  int

	key   []byte
	value []byte
	err   error
	valid bool
}

func (b block) iter() *blockIter {
	return &blockIter{data: b.data}
}

// next 前进到下一条记录，返回是否还有。
func (i *blockIter) next() bool {
	if i.err != nil || i.pos >= len(i.data) {
		i.valid = false
		return false
	}

	key, rest, err := takeLenPrefixed(i.data[i.pos:])
	if err != nil {
		i.err, i.valid = err, false
		return false
	}
	value, rest2, err := takeLenPrefixed(rest)
	if err != nil {
		i.err, i.valid = err, false
		return false
	}

	i.key, i.value = key, value
	i.pos = len(i.data) - len(rest2)
	i.valid = true
	return true
}

// seek 定位到第一个 >= target 的记录。
//
// 块内是顺序扫描的 —— 见本文件顶部的说明：块很小且已在内存，
// 扫描的成本远低于维护块内索引的复杂度。
func (i *blockIter) seek(target []byte) bool {
	i.pos = 0
	i.err = nil
	for i.next() {
		if ikey.Compare(i.key, target) >= 0 {
			return true
		}
	}
	return false
}

func (i *blockIter) Valid() bool   { return i.valid && i.err == nil }
func (i *blockIter) Key() []byte   { return i.key }
func (i *blockIter) Value() []byte { return i.value }
func (i *blockIter) Error() error  { return i.err }

// ── 变长编码辅助 ────────────────────────────────────────────

func appendLenPrefixed(dst, b []byte) []byte {
	var n [binary.MaxVarintLen64]byte
	w := binary.PutUvarint(n[:], uint64(len(b)))
	dst = append(dst, n[:w]...)
	return append(dst, b...)
}

func takeLenPrefixed(data []byte) (value, rest []byte, err error) {
	n, w := binary.Uvarint(data)
	if w <= 0 {
		return nil, nil, fmt.Errorf("%w: 长度字段损坏", ErrCorrupt)
	}
	data = data[w:]
	if uint64(len(data)) < n {
		return nil, nil, fmt.Errorf("%w: 声称长度 %d 但只剩 %d 字节", ErrCorrupt, n, len(data))
	}
	return data[:n], data[n:], nil
}
