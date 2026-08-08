package shale

import "fmt"

// Options 是打开数据库时的配置。
//
// 所有字段都可以留零值 —— Open 时会自动填上默认值。
// 只想改一两项时可以这样写：
//
//	db, err := shale.Open("/tmp/mydb", &shale.Options{SyncWAL: true})
type Options struct {
	// ── MemTable ─────────────────────────────────────────

	// MemTableSize 是单个 MemTable 的大小上限，超过就冻结并刷成 SSTable。
	//
	// 默认 4MB。这个值刻意比 RocksDB 的 64MB 小得多 ——
	// 学习项目要让 flush 和 compaction 频繁发生，才看得到现象。
	MemTableSize int64

	// MaxMemTables 是内存中最多允许存在几个 MemTable（含正在刷盘的）。
	// 达到上限后写入会被阻塞，直到刷盘腾出位置。默认 2。
	MaxMemTables int

	// ── Compaction ───────────────────────────────────────

	// L0CompactionTrigger 是 L0 攒够几个文件就触发合并。默认 4。
	//
	// 调小 → L0 文件少，读快，但 compaction 更频繁（写慢）
	// 调大 → 反之
	L0CompactionTrigger int

	// L0StopWritesTrigger 是 L0 文件数达到多少就【停止写入】。默认 12。
	// 这是最后的刹车：说明 compaction 已经严重跟不上写入了。
	L0StopWritesTrigger int

	// LevelBaseSize 是 L1 的总容量上限。默认 8MB。
	//
	// 注意 L0 的容量不由这里决定，而是
	// MemTableSize × L0CompactionTrigger —— 所以 L0 和 L1 大小相当，
	// "每层 10 倍"这个关系是从 L1 才开始的。
	LevelBaseSize int64

	// LevelSizeMultiplier 是从 L1 往下每层容量的放大倍数。默认 10。
	// L2 = L1 × 10，L3 = L2 × 10 ……
	LevelSizeMultiplier int

	// MaxLevels 是最大层数。默认 7（L0 ~ L6）。
	MaxLevels int

	// ── SSTable ──────────────────────────────────────────

	// SSTableSize 是单个 SSTable 文件的目标大小。默认 2MB。
	// compaction 输出超过这个大小就切一个新文件。
	SSTableSize int64

	// BlockSize 是 SSTable 内部 Data Block 的大小。默认 4KB。
	// 读取的最小单位就是一个 Block。
	BlockSize int

	// BloomBitsPerKey 是布隆过滤器给每个 key 分配几个 bit。默认 10（假阳性率约 0.8%）。
	// 设为 0 表示【关闭】布隆过滤器 —— 可以用来对比测试它到底省了多少次磁盘读。
	BloomBitsPerKey int

	// ── 缓存与持久化 ──────────────────────────────────────

	// BlockCacheSize 是 Block 缓存的容量。默认 8MB。设为 0 表示禁用缓存。
	BlockCacheSize int64

	// SyncWAL 决定每次写入是否 fsync。
	//
	//	false（默认）：只写进操作系统的页缓存。进程崩溃不丢数据，
	//	               但【机器断电会丢】最近的写入。速度快一个数量级。
	//	true         ：每次写都 fsync。断电也不丢已返回成功的写入。
	SyncWAL bool

	// ReadOnly 以只读模式打开，不会创建目录、不会写入、不会触发 compaction。
	ReadOnly bool
}

// 各配置项的默认值。
const (
	DefaultMemTableSize        = 4 << 20 // 4MB
	DefaultMaxMemTables        = 2
	DefaultL0CompactionTrigger = 4
	DefaultL0StopWritesTrigger = 12
	DefaultLevelBaseSize       = 8 << 20 // 8MB
	DefaultLevelSizeMultiplier = 10
	DefaultMaxLevels           = 7
	DefaultSSTableSize         = 2 << 20 // 2MB
	DefaultBlockSize           = 4 << 10 // 4KB
	DefaultBloomBitsPerKey     = 10
	DefaultBlockCacheSize      = 8 << 20 // 8MB
)

// DefaultOptions 返回一份全部取默认值的配置。
func DefaultOptions() *Options {
	o := &Options{}
	o.fillDefaults()
	return o
}

// fillDefaults 把零值字段填上默认值。就地修改。
func (o *Options) fillDefaults() {
	if o.MemTableSize <= 0 {
		o.MemTableSize = DefaultMemTableSize
	}
	if o.MaxMemTables <= 0 {
		o.MaxMemTables = DefaultMaxMemTables
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = DefaultL0CompactionTrigger
	}
	if o.L0StopWritesTrigger <= 0 {
		o.L0StopWritesTrigger = DefaultL0StopWritesTrigger
	}
	if o.LevelBaseSize <= 0 {
		o.LevelBaseSize = DefaultLevelBaseSize
	}
	if o.LevelSizeMultiplier <= 0 {
		o.LevelSizeMultiplier = DefaultLevelSizeMultiplier
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = DefaultMaxLevels
	}
	if o.SSTableSize <= 0 {
		o.SSTableSize = DefaultSSTableSize
	}
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
	// BloomBitsPerKey 和 BlockCacheSize 的 0 是有意义的（表示关闭），
	// 所以用负数判断"没设置"。
	if o.BloomBitsPerKey < 0 {
		o.BloomBitsPerKey = DefaultBloomBitsPerKey
	}
	if o.BlockCacheSize < 0 {
		o.BlockCacheSize = DefaultBlockCacheSize
	}
}

// validate 检查配置项之间的约束是否成立。
func (o *Options) validate() error {
	if o.L0StopWritesTrigger < o.L0CompactionTrigger {
		return fmt.Errorf("%w: L0StopWritesTrigger(%d) 不能小于 L0CompactionTrigger(%d)，"+
			"否则还没开始合并就停写了",
			ErrInvalidOptions, o.L0StopWritesTrigger, o.L0CompactionTrigger)
	}
	if o.LevelSizeMultiplier < 2 {
		return fmt.Errorf("%w: LevelSizeMultiplier(%d) 至少为 2，否则分层没有意义",
			ErrInvalidOptions, o.LevelSizeMultiplier)
	}
	if o.MaxLevels < 2 {
		return fmt.Errorf("%w: MaxLevels(%d) 至少为 2（L0 + L1）",
			ErrInvalidOptions, o.MaxLevels)
	}
	if int64(o.BlockSize) > o.SSTableSize {
		return fmt.Errorf("%w: BlockSize(%d) 不能大于 SSTableSize(%d)",
			ErrInvalidOptions, o.BlockSize, o.SSTableSize)
	}
	if o.BloomBitsPerKey > 64 {
		return fmt.Errorf("%w: BloomBitsPerKey(%d) 过大，超过 64 已无实际收益",
			ErrInvalidOptions, o.BloomBitsPerKey)
	}
	return nil
}

// LevelMaxBytes 返回第 level 层的容量上限。
//
//	L0：由 MemTableSize × L0CompactionTrigger 决定（它是个缓冲区，不参与逐层放大）
//	L1：LevelBaseSize
//	Ln：L(n-1) × LevelSizeMultiplier
func (o *Options) LevelMaxBytes(level int) int64 {
	if level <= 0 {
		return o.MemTableSize * int64(o.L0CompactionTrigger)
	}
	size := o.LevelBaseSize
	for i := 1; i < level; i++ {
		size *= int64(o.LevelSizeMultiplier)
	}
	return size
}

// clone 返回一份拷贝，避免 Open 之后调用方再改配置影响到已打开的 DB。
func (o *Options) clone() *Options {
	c := *o
	return &c
}
