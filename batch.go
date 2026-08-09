package shale

import (
	"encoding/binary"
	"fmt"

	"github.com/leyouhong/shale/internal/ikey"
)

// Batch 是一组要【原子生效】的写操作。
//
// 为什么需要它：一次业务操作可能要改多个 key（比如上层 SQL 的一行数据
// 对应「主键记录 + 若干索引项」），必须要么全成功、要么全失败，
// 否则会留下不一致的中间状态。
//
// 用法：
//
//	b := shale.NewBatch()
//	b.Put([]byte("k1"), []byte("v1"))
//	b.Delete([]byte("k2"))
//	err := db.Write(b)
//
// # 二进制格式
//
// Batch 的内部缓冲区【本身就是要写进 WAL 的字节】—— 不需要再转换一次格式。
// 这是 LevelDB 的设计，很省事：
//
//	┌───────────────┬──────────────┬─────────────────────────┐
//	│ seq (8B, LE)  │ count (4B,LE)│  record × count          │
//	└───────────────┴──────────────┴─────────────────────────┘
//
//	record（写入）：kind=1 │ varint(len(key)) │ key │ varint(len(val)) │ val
//	record（删除）：kind=0 │ varint(len(key)) │ key          ← 没有 value
//
// seq 在 Write 时才由 DB 填入 —— 整批共享同一个起始序号，
// 第 i 条记录的序号是 seq+i。这正是原子性在 key 编码层面的体现。
type Batch struct {
	buf []byte
}

// batchHeaderSize 是 seq(8) + count(4)。
const batchHeaderSize = 12

// NewBatch 创建一个空的 Batch。
func NewBatch() *Batch {
	b := &Batch{}
	b.Reset()
	return b
}

// Reset 清空 Batch 以便复用，避免反复分配内存。
func (b *Batch) Reset() {
	if cap(b.buf) >= batchHeaderSize {
		b.buf = b.buf[:batchHeaderSize]
	} else {
		b.buf = make([]byte, batchHeaderSize, 256)
	}
	// header 清零：seq 待填，count 从 0 开始
	for i := range b.buf[:batchHeaderSize] {
		b.buf[i] = 0
	}
}

// Put 往 Batch 里加一条写入。key 和 value 会被复制，调用后可以安全修改原切片。
func (b *Batch) Put(key, value []byte) {
	b.setCount(b.Count() + 1)
	b.buf = append(b.buf, byte(ikey.KindSet))
	b.buf = appendBytes(b.buf, key)
	b.buf = appendBytes(b.buf, value)
}

// Delete 往 Batch 里加一条删除（也就是写一个墓碑）。
func (b *Batch) Delete(key []byte) {
	b.setCount(b.Count() + 1)
	b.buf = append(b.buf, byte(ikey.KindDelete))
	b.buf = appendBytes(b.buf, key)
}

// Count 返回 Batch 里有多少条记录。
func (b *Batch) Count() int {
	return int(binary.LittleEndian.Uint32(b.buf[8:12]))
}

// Empty 判断 Batch 是否为空。
func (b *Batch) Empty() bool { return b.Count() == 0 }

// Size 返回编码后的字节数，可以用来估算这一批会占多少 WAL 空间。
func (b *Batch) Size() int { return len(b.buf) }

// Seq 返回这一批的起始序号（Write 之前是 0）。
func (b *Batch) Seq() uint64 {
	return binary.LittleEndian.Uint64(b.buf[0:8])
}

// SetSeq 设置起始序号。由 DB.Write 在真正写入前调用。
func (b *Batch) SetSeq(seq uint64) {
	binary.LittleEndian.PutUint64(b.buf[0:8], seq)
}

// Bytes 返回可以直接写进 WAL 的字节。返回的是内部缓冲区，不要修改。
func (b *Batch) Bytes() []byte { return b.buf }

// Load 从 WAL 读上来的字节恢复成 Batch，用于崩溃恢复时重放。
func (b *Batch) Load(data []byte) error {
	if len(data) < batchHeaderSize {
		return fmt.Errorf("%w: batch 长度 %d 小于头部大小 %d",
			ErrCorrupt, len(data), batchHeaderSize)
	}
	b.buf = append(b.buf[:0], data...)
	// 立刻遍历一遍做校验，别等到用的时候才发现数据是坏的
	if err := b.Iterate(func(_ ikey.Kind, _, _ []byte) error { return nil }); err != nil {
		return err
	}
	return nil
}

// Append 把另一个 Batch 的记录全部追加到自己后面。
//
// group commit 靠它把多个并发写入者的 batch 合成一个 ——
// 而这几乎是零成本的：记录区就是一段连续字节，直接拼接即可，
// 只需要把 count 加起来。seq 由 DB 在真正写入前统一分配。
//
// 这正是"Batch 的二进制形式就是 WAL 记录"这个设计的红利：
// 合并之后依然是一条合法的 WAL 记录，不需要任何转换。
func (b *Batch) Append(other *Batch) {
	if other == nil || other.Empty() {
		return
	}
	b.setCount(b.Count() + other.Count())
	b.buf = append(b.buf, other.buf[batchHeaderSize:]...)
}

// Iterate 按顺序遍历 Batch 里的每条记录。
//
// fn 返回非 nil 时立刻中止并把该错误返回。
// 传给 fn 的 key/value 是内部缓冲区的子切片，不要保存引用。
func (b *Batch) Iterate(fn func(kind ikey.Kind, key, value []byte) error) error {
	data := b.buf[batchHeaderSize:]
	want := b.Count()

	for i := 0; i < want; i++ {
		if len(data) == 0 {
			return fmt.Errorf("%w: batch 声称有 %d 条记录，实际只有 %d 条",
				ErrCorrupt, want, i)
		}

		kind := ikey.Kind(data[0])
		data = data[1:]

		key, rest, err := takeBytes(data)
		if err != nil {
			return fmt.Errorf("%w（第 %d 条记录的 key）", err, i)
		}
		data = rest

		var value []byte
		switch kind {
		case ikey.KindSet:
			value, rest, err = takeBytes(data)
			if err != nil {
				return fmt.Errorf("%w（第 %d 条记录的 value）", err, i)
			}
			data = rest
		case ikey.KindDelete:
			// 墓碑没有 value
		default:
			return fmt.Errorf("%w: 第 %d 条记录的 kind=%d 非法", ErrCorrupt, i, kind)
		}

		if err := fn(kind, key, value); err != nil {
			return err
		}
	}

	if len(data) != 0 {
		return fmt.Errorf("%w: batch 末尾有 %d 字节多余数据", ErrCorrupt, len(data))
	}
	return nil
}

func (b *Batch) setCount(n int) {
	binary.LittleEndian.PutUint32(b.buf[8:12], uint32(n))
}

// ── 变长编码辅助 ────────────────────────────────────────────
//
// 用 varint 而不是固定 4 字节存长度：绝大多数 key 都很短，
// varint 只花 1 字节就能表示 127 以内的长度，能省下可观的空间。

func appendBytes(dst, b []byte) []byte {
	var n [binary.MaxVarintLen64]byte
	w := binary.PutUvarint(n[:], uint64(len(b)))
	dst = append(dst, n[:w]...)
	return append(dst, b...)
}

func takeBytes(data []byte) (value, rest []byte, err error) {
	n, w := binary.Uvarint(data)
	if w <= 0 {
		return nil, nil, fmt.Errorf("%w: varint 长度字段损坏", ErrCorrupt)
	}
	data = data[w:]
	if uint64(len(data)) < n {
		return nil, nil, fmt.Errorf("%w: 声称长度 %d 但只剩 %d 字节",
			ErrCorrupt, n, len(data))
	}
	return data[:n], data[n:], nil
}
