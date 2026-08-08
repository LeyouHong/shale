package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/leyouhong/shale/internal/ikey"
)

// Reader 读取一个 SSTable 文件。
//
// 因为文件是只读不可变的，Reader 天然并发安全 —— 任意多个 goroutine
// 可以同时用同一个 Reader，不需要加锁。这是"写完就不改"带来的红利之一。
type Reader struct {
	r    io.ReaderAt
	c    io.Closer // 从文件打开时才有
	size int64

	// index 是稀疏索引，打开时一次性读进内存并常驻。
	// 它很小：每个 4KB 的数据块只占一条，1MB 的文件大约 250 条。
	index block

	firstKey []byte
	lastKey  []byte
	entries  int
	maxSeq   uint64
}

// Open 打开一个 SSTable 文件。
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r, err := NewReader(f, info.Size())
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: 打开 %s 失败: %w", path, err)
	}
	r.c = f
	return r, nil
}

// NewReader 从任意 ReaderAt 创建 Reader。
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	if size < footerSize {
		return nil, fmt.Errorf("%w: 文件只有 %d 字节，放不下 Footer", ErrCorrupt, size)
	}

	// ① 先读 Footer —— 它在文件末尾且长度固定，所以能直接定位。
	var footer [footerSize]byte
	if _, err := r.ReadAt(footer[:], size-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: 读 Footer 失败: %w", err)
	}
	if magic := binary.LittleEndian.Uint64(footer[32:40]); magic != magicNumber {
		return nil, fmt.Errorf("%w: Magic Number 不匹配（%016x），这不是一个 shale SSTable",
			ErrCorrupt, magic)
	}

	indexOff := binary.LittleEndian.Uint64(footer[0:8])
	indexSize := binary.LittleEndian.Uint64(footer[8:16])
	if indexOff+indexSize > uint64(size) {
		return nil, fmt.Errorf("%w: Index Block 位置越界", ErrCorrupt)
	}

	// ② 再据此读 Index Block，常驻内存。
	raw := make([]byte, indexSize)
	if _, err := r.ReadAt(raw, int64(indexOff)); err != nil {
		return nil, fmt.Errorf("sstable: 读 Index Block 失败: %w", err)
	}
	index, err := decodeBlock(raw)
	if err != nil {
		return nil, err
	}

	sr := &Reader{r: r, size: size, index: index}
	if err := sr.scanBounds(); err != nil {
		return nil, err
	}
	return sr, nil
}

// scanBounds 记录文件的 key 范围、记录数和最大序号。
//
// key 范围（最小/最大 key）在 M4 做分层时至关重要：
// 判断两个文件是否重叠、compaction 该卷入哪些文件，全靠它。
//
// 最大序号则用于重启时恢复全局 seq —— 见 MaxSeq 的说明。
func (r *Reader) scanBounds() error {
	it := r.NewIterator()
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if r.entries == 0 {
			r.firstKey = append([]byte(nil), it.Key()...)
		}
		r.lastKey = append(r.lastKey[:0], it.Key()...)
		// 注意不能只看第一条或最后一条：内部 key 按「user key 升序 +
		// seq 降序」排列，最大的 seq 可能出现在任何位置。
		if seq := ikey.Seq(it.Key()); seq > r.maxSeq {
			r.maxSeq = seq
		}
		r.entries++
	}
	return it.Error()
}

// Get 查找一个用户 key 在 snapshot 时刻的值。
//
// 返回值语义同 memtable.Get：
//
//	ikey.NotFound —— 这个文件里没有该 key，调用方应继续查更老的文件
//	ikey.Found    —— 找到了值
//	ikey.Deleted  —— 找到了墓碑，该 key 已删除，停止查找
func (r *Reader) Get(userKey []byte, snapshot uint64) ([]byte, ikey.Lookup, error) {
	var stack [72]byte
	seek := ikey.MakeSeekKey(stack[:0], userKey, snapshot)

	// ① 在稀疏索引里找到目标可能所在的那个数据块。
	//    索引项记的是每个块的最大 key，所以"第一个最大 key >= 目标"
	//    的那个块，就是唯一可能包含目标的块。
	h, ok := r.findBlock(seek)
	if !ok {
		return nil, ikey.NotFound, nil // 目标比文件里所有 key 都大
	}

	// ② 只读这一个块（这就是稀疏索引省下的 IO）。
	b, err := r.readBlock(h)
	if err != nil {
		return nil, ikey.NotFound, err
	}

	// ③ 块内顺序扫，找第一个 >= seek key 的记录。
	it := b.iter()
	if !it.seek(seek) {
		return nil, ikey.NotFound, it.Error()
	}

	// 落点可能已经跨到别的 user key 去了，说明目标不存在。
	if !bytes.Equal(ikey.UserKey(it.Key()), userKey) {
		return nil, ikey.NotFound, nil
	}
	if ikey.GetKind(it.Key()) == ikey.KindDelete {
		return nil, ikey.Deleted, nil
	}
	return it.Value(), ikey.Found, nil
}

// findBlock 在索引里查找第一个"最大 key >= target"的数据块。
func (r *Reader) findBlock(target []byte) (blockHandle, bool) {
	it := r.index.iter()
	for it.next() {
		if ikey.Compare(it.Key(), target) >= 0 {
			return decodeHandle(it.Value())
		}
	}
	return blockHandle{}, false
}

func decodeHandle(v []byte) (blockHandle, bool) {
	off, n := binary.Uvarint(v)
	if n <= 0 {
		return blockHandle{}, false
	}
	size, n2 := binary.Uvarint(v[n:])
	if n2 <= 0 {
		return blockHandle{}, false
	}
	return blockHandle{offset: off, size: size}, true
}

// readBlock 从磁盘读出一个块并校验。
//
// M8 会在这里插入 Block Cache —— 目前每次都真读磁盘。
func (r *Reader) readBlock(h blockHandle) (block, error) {
	if h.offset+h.size > uint64(r.size) {
		return block{}, fmt.Errorf("%w: 块位置越界", ErrCorrupt)
	}
	raw := make([]byte, h.size)
	if _, err := r.r.ReadAt(raw, int64(h.offset)); err != nil {
		return block{}, fmt.Errorf("sstable: 读块失败: %w", err)
	}
	return decodeBlock(raw)
}

// FirstKey 返回文件里最小的内部 key（文件为空时返回 nil）。
func (r *Reader) FirstKey() []byte { return r.firstKey }

// LastKey 返回文件里最大的内部 key。
func (r *Reader) LastKey() []byte { return r.lastKey }

// EntryCount 返回记录条数。
func (r *Reader) EntryCount() int { return r.entries }

// MaxSeq 返回文件里最大的序号。
//
// 重启时必须靠它恢复全局 seq：一旦数据落盘、对应的 WAL 被删除，
// 那批记录的序号就【只存在于 SSTable 里】了。
// 如果重启后 seq 从 0 重新开始，新写入会被判定为比磁盘上的老数据还旧，
// 读出来的就是老值 —— 这是个很隐蔽但致命的错误。
//
// M4 有了 Manifest 之后，seq 会直接记在元数据里，
// 不必再扫文件（那时这个方法仍可用于校验）。
func (r *Reader) MaxSeq() uint64 { return r.maxSeq }

// Size 返回文件字节数。
func (r *Reader) Size() int64 { return r.size }

// Close 关闭底层文件（如果是通过 Open 打开的）。
func (r *Reader) Close() error {
	if r.c != nil {
		return r.c.Close()
	}
	return nil
}

// ── 迭代器 ──────────────────────────────────────────────────

// Iterator 遍历 SSTable 里的【所有】记录，包括墓碑和旧版本。
//
// compaction 需要看到全部原始记录才能正确地做归并和清理，
// 所以这里不做任何过滤。
type Iterator struct {
	r *Reader

	indexIter *blockIter
	dataIter  *blockIter

	err error
}

// NewIterator 创建一个迭代器。初始无效，需要先 SeekToFirst 或 Seek。
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, indexIter: r.index.iter()}
}

// SeekToFirst 定位到第一条记录。
func (i *Iterator) SeekToFirst() {
	i.err = nil
	i.indexIter = i.r.index.iter()
	i.dataIter = nil
	if !i.indexIter.next() {
		return
	}
	if !i.loadCurrentBlock() {
		return
	}
	i.dataIter.next()
}

// Seek 定位到第一个 >= target 的记录。target 是【内部 key】。
func (i *Iterator) Seek(target []byte) {
	i.err = nil
	i.indexIter = i.r.index.iter()
	i.dataIter = nil

	// 先在索引里找到候选块，再在块内定位
	for i.indexIter.next() {
		if ikey.Compare(i.indexIter.Key(), target) >= 0 {
			if !i.loadCurrentBlock() {
				return
			}
			if i.dataIter.seek(target) {
				return
			}
			// 理论上不该发生（索引说这块的最大 key >= target），
			// 但真发生了就继续往后面的块找，保证行为正确。
			continue
		}
	}
	i.dataIter = nil
}

// Next 前进到下一条记录，必要时跨到下一个块。
func (i *Iterator) Next() {
	if i.dataIter == nil || i.err != nil {
		return
	}
	if i.dataIter.next() {
		return
	}
	if err := i.dataIter.Error(); err != nil {
		i.err = err
		return
	}
	// 当前块读完了，翻到下一块
	for i.indexIter.next() {
		if !i.loadCurrentBlock() {
			return
		}
		if i.dataIter.next() {
			return
		}
	}
	i.dataIter = nil
}

func (i *Iterator) loadCurrentBlock() bool {
	h, ok := decodeHandle(i.indexIter.Value())
	if !ok {
		i.err = fmt.Errorf("%w: 索引项损坏", ErrCorrupt)
		return false
	}
	b, err := i.r.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	i.dataIter = b.iter()
	return true
}

// Valid 返回当前位置是否有效。
func (i *Iterator) Valid() bool {
	return i.err == nil && i.dataIter != nil && i.dataIter.Valid()
}

// Key 返回当前的内部 key。
func (i *Iterator) Key() []byte { return i.dataIter.Key() }

// Value 返回当前的值。
func (i *Iterator) Value() []byte { return i.dataIter.Value() }

// Error 返回遍历中遇到的错误。
func (i *Iterator) Error() error {
	if i.err != nil {
		return i.err
	}
	if i.dataIter != nil {
		return i.dataIter.Error()
	}
	return nil
}

// Close 释放迭代器。
func (i *Iterator) Close() error { return i.err }
