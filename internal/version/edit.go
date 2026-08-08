// Package version 管理「当前有哪些文件、各在哪一层」这份元数据。
//
// 对应 LSM 原理的：第 8 步（分层管理）
//
// # 为什么不能靠扫目录
//
// M3 的做法是启动时 ls 一遍 *.sst。这在没有 compaction 时勉强能用，
// 但很快就不够了：
//
//	· compaction 会中途留下垃圾文件，目录里的文件不等于"生效的文件"
//	· 分层之后需要知道每个文件属于哪一层，文件名里没有这个信息
//	· 需要恢复全局 seq 和下一个文件号，扫目录得打开每个文件才知道
//	· 文件的 key 范围（判断重叠用）也得打开文件才能拿到，很慢
//
// 所以需要一份【权威记录】说明"此刻哪些文件是有效的"，这就是 Manifest。
//
// # 三个概念
//
//	FileMeta    —— 一个 SSTable 的元信息：编号、大小、key 范围
//	Version     —— 某一时刻的完整文件集合（按层组织），不可变
//	VersionEdit —— 从一个 Version 到下一个的【增量】：加了哪些文件、删了哪些
//
// Manifest 就是 VersionEdit 的日志。启动时从头重放，就重建出了当前 Version。
// 这和 WAL 重建 MemTable 是同一个套路 —— 记录变更而不是记录状态，
// 因为追加写远比覆盖写安全。
package version

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrCorrupt 表示 Manifest 内容损坏。
var ErrCorrupt = errors.New("version: 元数据损坏")

// 编码用的 tag。
//
// 用「tag + 值」而不是固定结构，是为了向前兼容：
// 以后加字段时，老版本读到不认识的 tag 可以选择跳过而不是直接崩溃。
const (
	tagLogNumber      = 1 // 当前 WAL 的编号
	tagNextFileNumber = 2 // 下一个可用的文件编号
	tagLastSequence   = 3 // 全局最大序号
	tagDeletedFile    = 4 // 删除一个文件：level + fileNum
	tagNewFile        = 5 // 新增一个文件：level + FileMeta
)

// FileMeta 是一个 SSTable 的元信息。
//
// 注意这里没有文件内容，只有"外壳"——但这些外壳信息足够回答
// compaction 最关心的问题：这个文件和那个文件的 key 范围有没有重叠。
type FileMeta struct {
	Num  uint64 // 文件编号，也是文件名
	Size int64  // 字节数

	// Smallest / Largest 是文件里最小和最大的【内部 key】。
	//
	// 分层 compaction 全靠它们：判断两个文件是否重叠、
	// 某一层里哪个文件可能包含目标 key，都是比较这两个边界。
	Smallest []byte
	Largest  []byte

	// MaxSeq 是文件里最大的序号，用于恢复全局 seq。
	MaxSeq uint64
}

// VersionEdit 描述一次版本变更。
//
// 它是【增量】而非全量：只记"加了什么、删了什么"。
// 一次 flush 产生一条（新增一个文件），
// 一次 compaction 也产生一条（删掉几个、新增几个）。
type VersionEdit struct {
	// 下面三个是可选字段，只在真的变化时才写进 Manifest。
	LogNumber    uint64
	NextFileNum  uint64
	LastSequence uint64
	hasLogNumber bool
	hasNextFile  bool
	hasLastSeq   bool

	// DeletedFiles 记录被移除的文件（层号 + 编号）。
	DeletedFiles []DeletedFile
	// NewFiles 记录新增的文件（层号 + 元信息）。
	NewFiles []NewFile
}

// DeletedFile 标记某一层里的某个文件被移除。
type DeletedFile struct {
	Level int
	Num   uint64
}

// NewFile 标记某一层里新增了一个文件。
type NewFile struct {
	Level int
	Meta  FileMeta
}

// SetLogNumber 设置当前 WAL 编号。
//
// 重启时靠它判断哪些 WAL 还需要重放：编号小于它的日志，
// 对应的数据已经落进 SSTable 了，可以直接跳过。
func (e *VersionEdit) SetLogNumber(n uint64) {
	e.LogNumber, e.hasLogNumber = n, true
}

// SetNextFileNum 设置下一个可用的文件编号。
func (e *VersionEdit) SetNextFileNum(n uint64) {
	e.NextFileNum, e.hasNextFile = n, true
}

// SetLastSequence 设置全局最大序号。
//
// 这一条极其关键：它让 seq 不再依赖"扫描 SSTable 内容"来恢复。
// M3 曾因为漏掉 seq 的持久化，导致 flush 后重启数据全查不到。
func (e *VersionEdit) SetLastSequence(n uint64) {
	e.LastSequence, e.hasLastSeq = n, true
}

// AddFile 登记一个新文件。
func (e *VersionEdit) AddFile(level int, m FileMeta) {
	e.NewFiles = append(e.NewFiles, NewFile{Level: level, Meta: m})
}

// DeleteFile 登记一个被移除的文件。
func (e *VersionEdit) DeleteFile(level int, num uint64) {
	e.DeletedFiles = append(e.DeletedFiles, DeletedFile{Level: level, Num: num})
}

// Empty 判断这条 edit 是否什么都没改。
func (e *VersionEdit) Empty() bool {
	return !e.hasLogNumber && !e.hasNextFile && !e.hasLastSeq &&
		len(e.DeletedFiles) == 0 && len(e.NewFiles) == 0
}

// Encode 把 edit 序列化成可以写进 Manifest 的字节。
func (e *VersionEdit) Encode() []byte {
	var buf []byte

	if e.hasLogNumber {
		buf = appendUvarint(buf, tagLogNumber)
		buf = appendUvarint(buf, e.LogNumber)
	}
	if e.hasNextFile {
		buf = appendUvarint(buf, tagNextFileNumber)
		buf = appendUvarint(buf, e.NextFileNum)
	}
	if e.hasLastSeq {
		buf = appendUvarint(buf, tagLastSequence)
		buf = appendUvarint(buf, e.LastSequence)
	}
	for _, d := range e.DeletedFiles {
		buf = appendUvarint(buf, tagDeletedFile)
		buf = appendUvarint(buf, uint64(d.Level))
		buf = appendUvarint(buf, d.Num)
	}
	for _, f := range e.NewFiles {
		buf = appendUvarint(buf, tagNewFile)
		buf = appendUvarint(buf, uint64(f.Level))
		buf = appendUvarint(buf, f.Meta.Num)
		buf = appendUvarint(buf, uint64(f.Meta.Size))
		buf = appendUvarint(buf, f.Meta.MaxSeq)
		buf = appendBytes(buf, f.Meta.Smallest)
		buf = appendBytes(buf, f.Meta.Largest)
	}
	return buf
}

// Decode 从字节还原一条 edit。
func (e *VersionEdit) Decode(data []byte) error {
	for len(data) > 0 {
		tag, n := binary.Uvarint(data)
		if n <= 0 {
			return fmt.Errorf("%w: tag 字段损坏", ErrCorrupt)
		}
		data = data[n:]

		var err error
		switch tag {
		case tagLogNumber:
			e.LogNumber, data, err = takeUvarint(data)
			e.hasLogNumber = true
		case tagNextFileNumber:
			e.NextFileNum, data, err = takeUvarint(data)
			e.hasNextFile = true
		case tagLastSequence:
			e.LastSequence, data, err = takeUvarint(data)
			e.hasLastSeq = true

		case tagDeletedFile:
			var level, num uint64
			if level, data, err = takeUvarint(data); err != nil {
				break
			}
			if num, data, err = takeUvarint(data); err != nil {
				break
			}
			e.DeletedFiles = append(e.DeletedFiles, DeletedFile{int(level), num})

		case tagNewFile:
			var level, num, size, maxSeq uint64
			var smallest, largest []byte
			if level, data, err = takeUvarint(data); err != nil {
				break
			}
			if num, data, err = takeUvarint(data); err != nil {
				break
			}
			if size, data, err = takeUvarint(data); err != nil {
				break
			}
			if maxSeq, data, err = takeUvarint(data); err != nil {
				break
			}
			if smallest, data, err = takeBytes(data); err != nil {
				break
			}
			if largest, data, err = takeBytes(data); err != nil {
				break
			}
			e.NewFiles = append(e.NewFiles, NewFile{
				Level: int(level),
				Meta: FileMeta{
					Num:      num,
					Size:     int64(size),
					MaxSeq:   maxSeq,
					Smallest: append([]byte(nil), smallest...),
					Largest:  append([]byte(nil), largest...),
				},
			})

		default:
			return fmt.Errorf("%w: 未知的 tag %d", ErrCorrupt, tag)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *VersionEdit) String() string {
	return fmt.Sprintf("VersionEdit{log:%d next:%d seq:%d +%d文件 -%d文件}",
		e.LogNumber, e.NextFileNum, e.LastSequence,
		len(e.NewFiles), len(e.DeletedFiles))
}

// ── 编码辅助 ────────────────────────────────────────────────

func appendUvarint(dst []byte, v uint64) []byte {
	var n [binary.MaxVarintLen64]byte
	w := binary.PutUvarint(n[:], v)
	return append(dst, n[:w]...)
}

func appendBytes(dst, b []byte) []byte {
	dst = appendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func takeUvarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, nil, fmt.Errorf("%w: varint 字段损坏", ErrCorrupt)
	}
	return v, data[n:], nil
}

func takeBytes(data []byte) ([]byte, []byte, error) {
	n, w := binary.Uvarint(data)
	if w <= 0 {
		return nil, nil, fmt.Errorf("%w: 长度字段损坏", ErrCorrupt)
	}
	data = data[w:]
	if uint64(len(data)) < n {
		return nil, nil, fmt.Errorf("%w: 声称 %d 字节但只剩 %d", ErrCorrupt, n, len(data))
	}
	return data[:n], data[n:], nil
}
