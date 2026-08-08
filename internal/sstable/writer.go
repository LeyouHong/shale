// Package sstable 实现 SSTable —— 磁盘上【内部有序、写完不改】的数据文件。
//
// 对应 LSM 原理的：第 3 步（让文件里的数据有序）
//
// # 为什么要有序
//
// 如果文件里的记录是乱序的，想查一个 key 就只能从头扫到尾。
// 排好序之后有两个好处：
//
//	① 内存里只需要【稀疏索引】—— 不用记每个 key 的位置，
//	   每块记一个就够了，内存占用降几百倍
//	② 范围查询直接顺着往下读
//
// # 文件长什么样
//
//	┌────────────────────────────────────┐
//	│  Data Block 1   (~4KB，块内 key 有序) │
//	│  Data Block 2                       │
//	│  ...                                │
//	├────────────────────────────────────┤
//	│  Index Block    每个 Data Block 的   │
//	│                 最大 key + 偏移量     │  ← 这就是稀疏索引
//	├────────────────────────────────────┤
//	│  Footer (固定 40 字节)               │
//	│    · Index Block 的位置              │
//	│    · Filter Block 的位置（M8 才用）   │
//	│    · Magic Number                   │
//	└────────────────────────────────────┘
//
// Footer 固定长度，所以打开文件时先从末尾读它，再据此找到 Index Block，
// 最后才知道数据块都在哪 —— 整个文件是【自描述】的。
//
// # 写完就不改
//
// SSTable 一旦落盘就永不修改。要改数据？写一条新记录到新文件里。
// 要删数据？写个墓碑。真正的清理交给 compaction 重写整个文件。
//
// 这条性质带来一串好处：不需要页内空闲空间管理、天然适合压缩、
// 只读文件可以被任意多个 goroutine 并发读而不用加锁。
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/leyouhong/shale/internal/ikey"
)

// ErrCorrupt 表示文件内容损坏。
var ErrCorrupt = errors.New("sstable: 文件内容损坏")

const (
	// footerSize 是文件尾部固定长度的元信息。
	// 固定长度是关键 —— 打开文件时才能"从末尾往回读 40 字节"直接拿到它。
	footerSize = 40

	// magicNumber 用来确认这确实是一个 shale 的 SSTable 文件，
	// 而不是别的什么东西（或者被截断了）。
	magicNumber uint64 = 0x5348414c45535354 // "SHALESST"

	// DefaultBlockSize 是 Data Block 的目标大小。
	DefaultBlockSize = 4 << 10
)

// blockHandle 指向文件里的一个块：偏移量 + 长度。
type blockHandle struct {
	offset uint64
	size   uint64 // 含尾部 CRC
}

// Writer 把一串【已经排好序】的记录写成一个 SSTable 文件。
//
// 调用方必须按内部 key 递增的顺序调用 Add —— 这是 SSTable 的根本前提，
// Writer 会检查并在乱序时报错。
type Writer struct {
	w   io.Writer
	off uint64 // 已写入的字节数，也就是下一个块的偏移

	blockSize int

	data  blockBuilder // 当前正在攒的数据块
	index blockBuilder // 索引块（每个数据块一条）

	entries  int
	lastKey  []byte
	firstKey []byte

	finished bool
	err      error
}

// NewWriter 创建一个 Writer。blockSize <= 0 时用默认值。
func NewWriter(w io.Writer, blockSize int) *Writer {
	if blockSize <= 0 {
		blockSize = DefaultBlockSize
	}
	return &Writer{w: w, blockSize: blockSize}
}

// Add 追加一条记录。key 必须是内部 key，且【严格递增】。
func (w *Writer) Add(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return errors.New("sstable: Writer 已经 Finish，不能再 Add")
	}

	// 顺序是 SSTable 的立身之本，写错了整个文件就废了 —— 必须当场发现。
	if w.entries > 0 && ikey.Compare(key, w.lastKey) <= 0 {
		w.err = fmt.Errorf("sstable: key 必须严格递增，%s 出现在 %s 之后",
			ikey.Debug(key), ikey.Debug(w.lastKey))
		return w.err
	}

	if w.entries == 0 {
		w.firstKey = append(w.firstKey[:0], key...)
	}
	w.data.add(key, value)
	w.lastKey = append(w.lastKey[:0], key...)
	w.entries++

	// 攒够一块就落盘。
	// 注意是"超过才切"，所以块可能略大于 blockSize —— 单条记录本身
	// 就可能超过一个块，这种情况必须允许，否则大 value 根本写不进去。
	if w.data.size() >= w.blockSize {
		return w.flushDataBlock()
	}
	return nil
}

// flushDataBlock 把当前数据块写出去，并在索引里登记一条。
func (w *Writer) flushDataBlock() error {
	if w.data.empty() {
		return nil
	}

	// 索引项记的是这个块的【最大 key】。
	// 查找时"第一个最大 key >= 目标"的块，就是目标可能所在的块。
	lastKey := append([]byte(nil), w.data.lastKey...)

	h, err := w.writeBlock(&w.data)
	if err != nil {
		return err
	}

	var buf [binary.MaxVarintLen64 * 2]byte
	n := binary.PutUvarint(buf[:], h.offset)
	n += binary.PutUvarint(buf[n:], h.size)
	w.index.add(lastKey, buf[:n])

	w.data.reset()
	return nil
}

// writeBlock 把一个块落盘，返回它的位置。
func (w *Writer) writeBlock(b *blockBuilder) (blockHandle, error) {
	raw := b.finish()
	if _, err := w.w.Write(raw); err != nil {
		w.err = err
		return blockHandle{}, err
	}
	h := blockHandle{offset: w.off, size: uint64(len(raw))}
	w.off += uint64(len(raw))
	return h, nil
}

// Finish 收尾：写出最后一个数据块、索引块和 Footer。
// 调用之后文件就完整了，可以关闭。
func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return nil
	}
	w.finished = true

	if err := w.flushDataBlock(); err != nil {
		return err
	}

	indexHandle, err := w.writeBlock(&w.index)
	if err != nil {
		return err
	}

	// Footer 固定 40 字节，且用定长整数而非 varint ——
	// 只有定长才能"从文件末尾往回数 40 字节"直接读到它。
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexHandle.offset)
	binary.LittleEndian.PutUint64(footer[8:16], indexHandle.size)
	binary.LittleEndian.PutUint64(footer[16:24], 0) // filter offset，M8 才用
	binary.LittleEndian.PutUint64(footer[24:32], 0) // filter size
	binary.LittleEndian.PutUint64(footer[32:40], magicNumber)

	if _, err := w.w.Write(footer[:]); err != nil {
		w.err = err
		return err
	}
	w.off += footerSize
	return nil
}

// FileSize 返回已写出的总字节数。
func (w *Writer) FileSize() int64 { return int64(w.off) }

// EntryCount 返回已写入的记录条数。
func (w *Writer) EntryCount() int { return w.entries }

// FirstKey 返回文件里最小的内部 key。
// M4 的层级管理会用它来判断文件之间的 key 范围是否重叠。
func (w *Writer) FirstKey() []byte { return w.firstKey }

// LastKey 返回文件里最大的内部 key。
func (w *Writer) LastKey() []byte { return w.lastKey }
