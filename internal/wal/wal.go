// Package wal 实现预写日志（Write-Ahead Log）。
//
// 对应 LSM 原理的：第 5 步（程序崩了怎么办）
//
// # 它解决什么问题
//
// MemTable 在内存里，进程一挂、机器一断电，那批数据就没了。
// 办法是：**写内存之前，先往一个日志文件里顺序追加一条**。
//
//	Put(k, v)  →  ① 追加到 WAL（磁盘，顺序写）
//	           →  ② 写进 MemTable（内存）
//	           →  ③ 返回成功
//
// 顺序不能反：先写内存再写日志的话，两步之间崩溃就会出现
// 「内存里有、日志里没有」，而内存里的东西马上也没了 —— 数据凭空消失。
//
// 恢复时把日志从头重放一遍，MemTable 就回来了。
//
// # 为什么要分块
//
// 崩溃可能发生在写入的【任何一个字节中间】，所以文件尾部大概率是半条记录。
// 恢复时必须能准确判断"读到哪里为止是完好的"。
//
// 做法是把文件切成固定 32KB 的块，每条记录带上长度和 CRC 校验：
//
//	┌──────────┬────────┬────────┬─────────────────┐
//	│ CRC32 4B │ Len 2B │ Type 1B│  Payload        │
//	└──────────┴────────┴────────┴─────────────────┘
//	                     ↑ FULL / FIRST / MIDDLE / LAST
//
// 一条记录放不下时会跨块，用 Type 标出它是完整的、还是某个分片。
// 这样即使尾部有残缺，也能一路读到最后一条完整记录为止，把损坏隔离在尾巴上。
//
// 块的剩余空间不足以放下 7 字节头部时，用 0 填满、换下一块。
//
// # 这个格式来自 LevelDB
//
// 它是被工业界验证过的成熟设计，本项目直接沿用。
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// BlockSize 是日志的分块大小。
	BlockSize = 32 << 10

	// headerSize = CRC32(4) + Length(2) + Type(1)
	headerSize = 7

	// MaxFragmentSize 是单个【分片】的最大长度：一个块减去头部。
	//
	// 注意这限制的是分片，不是逻辑记录 —— 记录多长都行，
	// 放不下就切成多个分片跨块存放，这正是分块格式的意义。
	// 因为分片必然小于 32KB，2 字节的长度字段足够用。
	MaxFragmentSize = BlockSize - headerSize
)

// recordType 标记一条物理记录是完整的，还是某条逻辑记录的分片。
type recordType uint8

const (
	// typeZero 是块尾填充用的，不是有效记录。
	typeZero recordType = 0
	// typeFull 表示一条完整的记录就在这里。
	typeFull recordType = 1
	// typeFirst 是跨块记录的第一个分片。
	typeFirst recordType = 2
	// typeMiddle 是中间分片。
	typeMiddle recordType = 3
	// typeLast 是最后一个分片。
	typeLast recordType = 4
)

func (t recordType) String() string {
	switch t {
	case typeZero:
		return "ZERO"
	case typeFull:
		return "FULL"
	case typeFirst:
		return "FIRST"
	case typeMiddle:
		return "MIDDLE"
	case typeLast:
		return "LAST"
	default:
		return fmt.Sprintf("Type(%d)", uint8(t))
	}
}

// ErrCorrupt 表示日志内容损坏（CRC 不匹配、分片顺序非法等）。
//
// 注意它和「尾部截断」不是一回事：截断是崩溃时的正常现象，
// Reader 会当作正常结束（io.EOF）；ErrCorrupt 则意味着数据真的坏了。
var ErrCorrupt = errors.New("wal: 日志内容损坏")

// crcTable 用 Castagnoli 多项式 —— 现代 CPU 有硬件指令加速，比 IEEE 快得多。
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ── Writer ─────────────────────────────────────────────────

// Writer 往日志文件追加记录。
//
// 每次 Write 都会把数据直接交给底层的 io.Writer，【不做缓冲】——
// 因为 WAL 的意义就是"返回成功时数据已经落到操作系统"。
// 攒在用户态缓冲区里的话，进程一崩就没了，等于白写。
type Writer struct {
	w io.Writer

	// blockOffset 是当前在块内的位置，决定还能塞多少内容、要不要换块。
	blockOffset int

	// written 是累计写入的字节数。
	written int64
}

// NewWriter 创建一个从文件开头写起的 Writer。
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// NewWriterAt 创建一个从已有文件的 offset 处【继续追加】的 Writer。
//
// 恢复流程会用到它：把日志尾部的残片截掉之后，
// 需要从那个位置接着往下写，而 Writer 必须知道自己在块内的位置才能正确分块。
func NewWriterAt(w io.Writer, offset int64) *Writer {
	return &Writer{
		w:           w,
		blockOffset: int(offset % BlockSize),
		written:     offset,
	}
}

// Write 追加一条记录。记录内容会被原样保存，长度可以为 0。
func (w *Writer) Write(record []byte) error {
	// 记录长度不设上限：超过一个块就自动切成多个分片。

	// 空记录也要写出去（一个 0 长度的 FULL 分片），
	// 所以这里用 do-while 的结构，而不是 for len(rest) > 0。
	rest := record
	first := true

	for {
		avail := BlockSize - w.blockOffset

		// 块尾连头部都放不下了 —— 用 0 填满，换下一块。
		// 恢复时读到全 0 的头部就知道该跳到下一块了。
		if avail < headerSize {
			if avail > 0 {
				var pad [headerSize]byte
				if _, err := w.w.Write(pad[:avail]); err != nil {
					return err
				}
				w.written += int64(avail)
			}
			w.blockOffset = 0
			avail = BlockSize
		}

		chunk := avail - headerSize
		n := len(rest)
		if n > chunk {
			n = chunk
		}
		last := n == len(rest)

		var t recordType
		switch {
		case first && last:
			t = typeFull
		case first:
			t = typeFirst
		case last:
			t = typeLast
		default:
			t = typeMiddle
		}

		if err := w.emit(t, rest[:n]); err != nil {
			return err
		}

		rest = rest[n:]
		first = false
		if last {
			return nil
		}
	}
}

// emit 写出一个物理分片：头部 + 内容。
func (w *Writer) emit(t recordType, payload []byte) error {
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint16(hdr[4:6], uint16(len(payload)))
	hdr[6] = byte(t)

	// CRC 覆盖 type 和 payload。
	// 把 type 也算进去，才能发现"分片类型被改坏"这种损坏。
	crc := crc32.Update(0, crcTable, hdr[6:7])
	crc = crc32.Update(crc, crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[0:4], crc)

	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.w.Write(payload); err != nil {
			return err
		}
	}

	w.blockOffset += headerSize + len(payload)
	w.written += int64(headerSize + len(payload))
	return nil
}

// Size 返回已写入的总字节数。
func (w *Writer) Size() int64 { return w.written }

// ── Reader ─────────────────────────────────────────────────

// Reader 顺序读取日志里的记录。
//
// 崩溃恢复的场景决定了它的核心行为：**尾部残缺是正常的，不是错误**。
// 读到不完整的尾巴时返回 io.EOF，并可以通过 Truncated 和 ValidSize
// 查到"到哪里为止是完好的"。
type Reader struct {
	r io.Reader

	buf   [BlockSize]byte
	block []byte // 当前块中尚未消费的部分
	pos   int    // 在 block 里的位置

	// scratch 用来拼接跨块的记录。
	scratch []byte

	// validSize 是最后一条【完整记录】结束时的文件偏移。
	// 恢复时把文件截断到这里，就能安全地接着往下写。
	validSize int64

	// consumed 是已经读过的字节数（含尚未构成完整记录的部分）。
	consumed int64

	truncated bool
	done      bool
}

// NewReader 创建一个 Reader。
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// Next 返回下一条记录。
//
// 正常读完返回 io.EOF；尾部有残片时也返回 io.EOF（并把 Truncated 置为 true）；
// 只有内容真的损坏时才返回 ErrCorrupt。
//
// 返回的切片在下次调用 Next 之前有效，需要保留请自行复制。
func (r *Reader) Next() ([]byte, error) {
	if r.done {
		return nil, io.EOF
	}

	r.scratch = r.scratch[:0]
	inFragment := false

	for {
		payload, t, err := r.readChunk()
		if err != nil {
			// 尾部读不出完整分片 —— 崩溃时的正常现象
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if inFragment {
					// 记录只写了一半就崩了，这半条不能算数
					r.truncated = true
				}
				r.done = true
				return nil, io.EOF
			}
			r.done = true
			return nil, err
		}

		switch t {
		case typeFull:
			if inFragment {
				r.done = true
				return nil, fmt.Errorf("%w: 分片中途出现 FULL 记录", ErrCorrupt)
			}
			r.validSize = r.consumed
			return payload, nil

		case typeFirst:
			if inFragment {
				r.done = true
				return nil, fmt.Errorf("%w: 分片中途又出现 FIRST", ErrCorrupt)
			}
			r.scratch = append(r.scratch, payload...)
			inFragment = true

		case typeMiddle:
			if !inFragment {
				r.done = true
				return nil, fmt.Errorf("%w: 没有 FIRST 就出现 MIDDLE", ErrCorrupt)
			}
			r.scratch = append(r.scratch, payload...)

		case typeLast:
			if !inFragment {
				r.done = true
				return nil, fmt.Errorf("%w: 没有 FIRST 就出现 LAST", ErrCorrupt)
			}
			r.scratch = append(r.scratch, payload...)
			r.validSize = r.consumed
			return r.scratch, nil

		default:
			r.done = true
			return nil, fmt.Errorf("%w: 未知的分片类型 %d", ErrCorrupt, t)
		}
	}
}

// readChunk 读出一个物理分片。
func (r *Reader) readChunk() ([]byte, recordType, error) {
	for {
		// 块内剩余不足一个头部 —— 说明是块尾填充，换下一块
		if len(r.block)-r.pos < headerSize {
			r.consumed += int64(len(r.block) - r.pos) // 填充字节也算已消费
			if err := r.nextBlock(); err != nil {
				return nil, 0, err
			}
			continue
		}

		hdr := r.block[r.pos : r.pos+headerSize]
		length := int(binary.LittleEndian.Uint16(hdr[4:6]))
		t := recordType(hdr[6])

		// 全 0 的头部就是块尾填充
		if t == typeZero && length == 0 {
			r.consumed += int64(len(r.block) - r.pos)
			if err := r.nextBlock(); err != nil {
				return nil, 0, err
			}
			continue
		}

		if r.pos+headerSize+length > len(r.block) {
			// 声称的长度超出了本块 —— 只可能是写到一半崩了
			return nil, 0, io.ErrUnexpectedEOF
		}

		payload := r.block[r.pos+headerSize : r.pos+headerSize+length]

		want := binary.LittleEndian.Uint32(hdr[0:4])
		crc := crc32.Update(0, crcTable, hdr[6:7])
		crc = crc32.Update(crc, crcTable, payload)
		if crc != want {
			return nil, 0, fmt.Errorf("%w: CRC 校验失败（期望 %08x，实际 %08x）",
				ErrCorrupt, want, crc)
		}

		r.pos += headerSize + length
		r.consumed += int64(headerSize + length)
		return payload, t, nil
	}
}

// nextBlock 读入下一个块。
func (r *Reader) nextBlock() error {
	n, err := io.ReadFull(r.r, r.buf[:])
	switch {
	case err == nil:
		// 读满了一整块
	case errors.Is(err, io.EOF):
		return io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		// 最后一块不完整 —— 正常，文件本来就不一定是 32KB 的整数倍
		if n < headerSize {
			// 连一个头部都不够，剩下的全是残渣
			r.truncated = n > 0
			return io.EOF
		}
	default:
		return err
	}

	r.block = r.buf[:n]
	r.pos = 0
	return nil
}

// ValidSize 返回最后一条完整记录结束时的文件偏移。
//
// 恢复流程应当把日志文件截断到这个长度，再从这里继续追加 ——
// 这样就把崩溃留下的残片干净地切掉了。
func (r *Reader) ValidSize() int64 { return r.validSize }

// Truncated 返回日志尾部是否有残缺。
// 崩溃后重启时这通常是 true，属于正常现象。
func (r *Reader) Truncated() bool { return r.truncated }
