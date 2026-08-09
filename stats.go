package shale

import (
	"fmt"
	"strings"
)

// Stats 是数据库的运行状态快照。
//
// 这不只是给运维看的 —— 对学习项目来说，它是【观察 LSM 行为的窗口】：
// 灌一批数据进去，打印一次 Stats，就能亲眼看到文件怎么从 L0 一层层沉下去。
type Stats struct {
	// MemTableSize 是当前可写 MemTable 已占用的字节数。
	MemTableSize int64

	// ImmutableCount 是等待刷盘的 Immutable MemTable 个数。
	// 长期大于 0 说明刷盘跟不上写入。
	ImmutableCount int

	// Levels 是各层的统计，下标即层号（Levels[0] 就是 L0）。
	Levels []LevelStats

	// UserBytesWritten 是调用方实际写入的字节数。
	UserBytesWritten int64

	// DiskBytesWritten 是落到磁盘上的总字节数（含 WAL、flush、compaction）。
	//
	// DiskBytesWritten / UserBytesWritten 就是【写放大倍数】——
	// LSM 最核心的代价指标，理论上应该在 10~30 之间。
	DiskBytesWritten int64

	// FlushCount 是已完成的 flush 次数（MemTable → SSTable）。
	FlushCount int64

	// CompactionCount 是已完成的 compaction 次数。
	CompactionCount int64

	// WriteStalls 是写入被迫等待后台的次数。
	//
	// 这个数字持续增长说明后台跟不上前台 —— 要么调大 MemTable、
	// 要么放宽 L0 触发阈值，要么就是磁盘不够快。
	WriteStalls int64

	// BlockCacheHits / BlockCacheMisses 用于计算缓存命中率。
	BlockCacheHits   int64
	BlockCacheMisses int64

	// BloomFilterChecks 是布隆过滤器被查询的次数，
	// BloomFilterSkips 是它成功挡下（回答"不存在"）的次数。
	//
	// Skips / Checks 越高说明过滤器省掉的磁盘读越多。
	// 专门查一批不存在的 key，这个比值应该接近 1。
	BloomFilterChecks int64
	BloomFilterSkips  int64
}

// LevelStats 是单个层级的统计。
type LevelStats struct {
	Level    int   // 层号
	NumFiles int   // 该层的 SSTable 文件数
	Size     int64 // 该层总字节数
	MaxBytes int64 // 该层的容量上限
}

// Score 返回这一层的"拥挤程度"：实际大小 / 容量上限。
// 大于 1 表示超出容量，该做 compaction 了。compaction 调度器优先处理分数最高的层。
func (l LevelStats) Score() float64 {
	if l.MaxBytes <= 0 {
		return 0
	}
	return float64(l.Size) / float64(l.MaxBytes)
}

// WriteAmplification 返回写放大倍数。没有写入时返回 0。
func (s Stats) WriteAmplification() float64 {
	if s.UserBytesWritten == 0 {
		return 0
	}
	return float64(s.DiskBytesWritten) / float64(s.UserBytesWritten)
}

// BlockCacheHitRate 返回 Block 缓存命中率。
func (s Stats) BlockCacheHitRate() float64 {
	total := s.BlockCacheHits + s.BlockCacheMisses
	if total == 0 {
		return 0
	}
	return float64(s.BlockCacheHits) / float64(total)
}

// String 以表格形式输出，方便直接打印观察。
func (s Stats) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "MemTable: %s (immutable: %d)\n",
		humanBytes(s.MemTableSize), s.ImmutableCount)

	b.WriteString("\n  层级    文件数        大小       容量上限    Score\n")
	b.WriteString("  ────────────────────────────────────────────────────\n")
	for _, l := range s.Levels {
		fmt.Fprintf(&b, "  L%-4d %7d %11s %12s %8.2f\n",
			l.Level, l.NumFiles, humanBytes(l.Size), humanBytes(l.MaxBytes), l.Score())
	}

	fmt.Fprintf(&b, "\n写放大: %.2fx (用户写入 %s，磁盘写入 %s)\n",
		s.WriteAmplification(), humanBytes(s.UserBytesWritten), humanBytes(s.DiskBytesWritten))
	fmt.Fprintf(&b, "Compaction 次数: %d，写入等待次数: %d\n",
		s.CompactionCount, s.WriteStalls)

	if s.BlockCacheHits+s.BlockCacheMisses > 0 {
		fmt.Fprintf(&b, "Block 缓存命中率: %.1f%%\n", s.BlockCacheHitRate()*100)
	}
	if s.BloomFilterChecks > 0 {
		fmt.Fprintf(&b, "布隆过滤器: 查询 %d 次，挡下 %d 次 (%.1f%%)\n",
			s.BloomFilterChecks, s.BloomFilterSkips,
			float64(s.BloomFilterSkips)/float64(s.BloomFilterChecks)*100)
	}
	return b.String()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
