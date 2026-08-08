package shale

import "errors"

var (
	// ErrNotFound 表示 key 不存在，或者已经被删除。
	//
	// 注意区分两种"空"：
	//   Put(k, []byte{})  之后 Get(k) 返回 (空切片, nil)     —— key 存在，值为空
	//   Delete(k)         之后 Get(k) 返回 (nil, ErrNotFound) —— key 不存在
	ErrNotFound = errors.New("shale: not found")

	// ErrClosed 表示 DB 已经关闭，不能再操作。
	ErrClosed = errors.New("shale: database is closed")

	// ErrCorrupt 表示磁盘上的数据损坏（CRC 校验失败、格式非法等）。
	ErrCorrupt = errors.New("shale: corrupted data")

	// ErrKeyTooLarge 表示 key 超过了长度上限。
	// key 会被完整复制进内部 key、写进 WAL、写进每个 SSTable 的索引块，
	// 过大的 key 会显著拖垮各处，所以设一个硬上限。
	ErrKeyTooLarge = errors.New("shale: key too large")

	// ErrValueTooLarge 表示单条 value 超过了 MemTableSize，
	// 这种情况下它永远无法被正常刷盘。
	ErrValueTooLarge = errors.New("shale: value too large")

	// ErrInvalidOptions 表示 Options 里的配置项不合法。
	ErrInvalidOptions = errors.New("shale: invalid options")

	// ErrNotImplemented 表示该功能还没实现（当前里程碑尚未覆盖）。
	// 随着里程碑推进，返回这个错误的地方会逐个消失。
	ErrNotImplemented = errors.New("shale: not implemented yet")
)

// MaxKeySize 是单个 key 的长度上限（64KB）。
const MaxKeySize = 64 << 10
