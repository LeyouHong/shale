// Package shale 是一个用 Go 从零实现的 LSM Tree 存储引擎。
//
// shale（页岩）是层状沉积岩，由沉积物一层层压实而成 ——
// 正对应 LSM 的分层下渗与 compaction 压实。
//
// # 现在能用到哪一步
//
// 项目按里程碑推进（见 DESIGN.md 第五节），当前处于 M7：
// 读、写、扫描、崩溃恢复、分层 compaction 都已具备，
// 是一个结构完整的 LSM 引擎了。
//
// 还差两块性能件：布隆过滤器（M8，让"查不存在的 key"不必读盘）
// 和并发优化（M9，目前写入串行、compaction 同步执行）。
//
//	M0 ✓ 骨架、Options、错误、内部 key 编码、Batch 格式
//	M1 ✓ 跳表 + MemTable（纯内存的读写路径）
//	M2 ✓ WAL + 崩溃恢复
//	M3 ✓ SSTable 读写 + flush
//	M4 ✓ Manifest + Version + 文件引用计数
//	M5 ✓ Iterator + 多路归并
//	M6 ✓ Compaction（简单全量合并）
//	M7 ✓ Leveled Compaction + 分层
//	M8   布隆过滤器 + Block Cache
//	M9   并发 + 压测
//
// 还没实现的接口会返回 ErrNotImplemented（NewIterator / Flush / CompactAll）。
//
// # 用法
//
//	db, err := shale.Open("/tmp/mydb", nil)
//	if err != nil { ... }
//	defer db.Close()
//
//	db.Put([]byte("hello"), []byte("world"))
//	v, err := db.Get([]byte("hello"))
package shale

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/memtable"
	"github.com/leyouhong/shale/internal/version"
	"github.com/leyouhong/shale/internal/wal"
)

// DB 是一个打开的数据库实例。
//
// 所有方法都是并发安全的（M9 之前靠一把大锁保证，之后再优化）。
type DB struct {
	dir  string
	opts *Options

	mu     sync.RWMutex
	closed bool

	// seq 是全局单调递增的序号，每条写入记录分配一个。
	// 它决定了同一个 key 的多个版本谁新谁旧，是 LSM 的时间轴。
	// 重启时必须从 Manifest / WAL 恢复出正确的值，否则新写入会被误判为旧版本。
	seq uint64

	// mem 是当前可写的 MemTable，所有写入先落在这里。
	mem *memtable.MemTable

	// WAL：写入 MemTable 之前先把 batch 追加到这里，用于崩溃恢复。
	walFile *os.File
	wal     *wal.Writer
	walNum  uint64

	// recovered 是启动时从 WAL 重放出来的记录条数。
	recovered int

	// recoverWarning 记录恢复过程中遇到的问题（比如日志尾部损坏）。
	// 不是致命错误 —— 已经恢复的数据仍然可用 —— 但调用方应该知道。
	recoverWarning error

	// vs 管理元数据：当前有哪些文件、各在哪一层、seq 和文件编号。
	// 「有哪些文件」的唯一权威来源 —— 目录里的文件不算数，Manifest 说了算。
	vs *version.VersionSet

	// tables 缓存已打开的 SSTable 句柄，避免每次查询都重新打开文件。
	tables tableCache

	// compactPointer 记录每层上次 compaction 停在哪个 key，用于轮转。
	// 不轮转的话同一段 key 会被反复重写，后面的数据永远沉不下去。
	compactPointer [version.MaxLevels][]byte

	// 统计
	flushCount       int64
	compactionCount  int64
	droppedEntries   int64
	diskBytesWritten int64
	userBytesWritten int64
}

// Open 打开（或创建）一个数据库。
//
// dir 是存放所有数据文件的目录，不存在会自动创建。
// opts 传 nil 表示全部使用默认配置。
func Open(dir string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	opts = opts.clone()
	opts.fillDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("shale: 解析路径失败: %w", err)
	}

	if opts.ReadOnly {
		info, err := os.Stat(abs)
		if err != nil {
			return nil, fmt.Errorf("shale: 只读模式下目录必须已存在: %w", err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("shale: %s 不是目录", abs)
		}
	} else {
		if err := os.MkdirAll(abs, 0o755); err != nil {
			return nil, fmt.Errorf("shale: 创建目录失败: %w", err)
		}
		// 立刻验证目录真的可写，别等到第一次写入才失败
		if err := checkWritable(abs); err != nil {
			return nil, err
		}
	}

	db := &DB{
		dir:    abs,
		opts:   opts,
		mem:    memtable.New(),
		tables: make(tableCache),
		vs:     version.NewVersionSet(abs),
	}

	// ① 先从 Manifest 恢复元数据：有哪些文件、seq 到哪了、下一个文件号是几。
	//
	//    这一步替代了 M3 的「扫描 *.sst」——
	//    目录里的文件只是物理存在，Manifest 登记过的才是逻辑生效的。
	//    崩溃时留下的孤儿文件因此会被自然忽略。
	if _, err := db.vs.Recover(); err != nil {
		return nil, err
	}
	db.seq = db.vs.LastSequence()

	// ② 再重放 WAL：那是还没来得及落盘的部分，比 SSTable 里的更新。
	if err := db.recover(); err != nil {
		db.closeWAL()
		db.closeTables()
		return nil, err
	}

	if !opts.ReadOnly {
		// ③ 换一个新的 Manifest（内容是当前状态的完整快照）。
		//    每次启动都这么做，Manifest 才不会无限增长。
		if err := db.vs.CreateManifest(db.walNum); err != nil {
			db.closeWAL()
			return nil, err
		}
		db.vs.SetLastSequence(db.seq)

		// ④ 清掉孤儿文件：没被 Manifest 登记的 .sst、已经落盘的旧 WAL、
		//    以及上次崩溃留下的 .tmp 残片。
		if err := cleanupTempFiles(abs); err != nil {
			return nil, fmt.Errorf("shale: 清理临时文件失败: %w", err)
		}
		if err := db.cleanupObsoleteFiles(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// RecoveredEntries 返回启动时从 WAL 重放出来的记录条数。
func (db *DB) RecoveredEntries() int { return db.recovered }

// RecoverWarning 返回恢复过程中遇到的问题，没有问题时返回 nil。
//
// 典型情况是日志尾部损坏 —— 此时已恢复的数据仍然可用，
// 但调用方可能想记录一条告警。
func (db *DB) RecoverWarning() error { return db.recoverWarning }

// Close 关闭数据库，释放所有资源。
// 关闭后再调用任何方法都会返回 ErrClosed。重复 Close 是安全的。
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	// M9 起：这里还要等后台 flush / compaction 的 goroutine 退出
	err := db.closeWAL()
	if cerr := db.closeTables(); err == nil {
		err = cerr
	}
	if cerr := db.vs.Close(); err == nil {
		err = cerr
	}
	return err
}

// Dir 返回数据目录的绝对路径。
func (db *DB) Dir() string { return db.dir }

// Put 写入一对 key-value。相同的 key 再写一次即为覆盖。
func (db *DB) Put(key, value []byte) error {
	b := NewBatch()
	b.Put(key, value)
	return db.Write(b)
}

// Delete 删除一个 key。
//
// 注意：这不会真的把数据从磁盘上抹掉，而是追加一条【墓碑】记录。
// 数据要等到 compaction 把墓碑推到最底层时才真正消失。
// 删除一个不存在的 key 不算错误。
func (db *DB) Delete(key []byte) error {
	b := NewBatch()
	b.Delete(key)
	return db.Write(b)
}

// Get 读取一个 key 的值。
//
// key 不存在或已被删除时返回 ErrNotFound。
// 返回的切片是数据的拷贝，调用方可以自由修改。
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}

	// LSM 的读路径：【从新到旧】逐层查找，谁先给出确定答案就用谁的。
	//
	//	MemTable（最新）→ SSTable（编号从大到小，越大越新）
	//
	// 「确定的答案」包括墓碑 —— 遇到它必须立刻返回"不存在"，
	// 绝不能因为"没拿到值"就继续往下找，否则会把已删除的数据读回来。
	snapshot := db.seq

	// ① 内存里最新的那一份
	if value, res := db.mem.Get(key, snapshot); res != ikey.NotFound {
		if res == ikey.Deleted {
			return nil, ErrNotFound
		}
		// MemTable 里的 value 指向内部存储，必须复制一份再交给调用方。
		return append([]byte(nil), value...), nil
	}

	// ② 再逐个问已落盘的 SSTable
	value, res, err := db.getFromTables(db.vs.Current(), key, snapshot)
	if err != nil {
		return nil, err
	}
	switch res {
	case ikey.Found:
		return append([]byte(nil), value...), nil
	default:
		// Deleted 和 NotFound 对调用方是同一件事：这个 key 现在不存在。
		return nil, ErrNotFound
	}
}

// Write 原子地执行一批操作。
func (db *DB) Write(b *Batch) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if b == nil || b.Empty() {
		return nil
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	// 先校验，避免写了一半才发现某条记录非法
	if err := b.Iterate(func(_ ikey.Kind, key, value []byte) error {
		if err := validateKey(key); err != nil {
			return err
		}
		if int64(len(value)) > db.opts.MemTableSize {
			return fmt.Errorf("%w: value %d 字节超过 MemTableSize %d",
				ErrValueTooLarge, len(value), db.opts.MemTableSize)
		}
		return nil
	}); err != nil {
		return err
	}

	// 给整批分配一段连续的序号：第 i 条记录拿到 base+i。
	//
	// 「整批共享一个起始 seq」正是原子性在 key 编码层面的落地 ——
	// 崩溃恢复时只要看 WAL 里这条 batch 记录是否完整，
	// 就知道这一批是全部生效还是全部没生效。
	base := db.seq + 1
	b.SetSeq(base)

	// ① 先落日志。顺序绝不能反：
	//    先写 MemTable 的话，两步之间崩溃就会出现「内存里有、日志里没有」，
	//    而内存里的东西马上也没了 —— 数据凭空消失。
	if db.wal != nil {
		if err := db.wal.Write(b.Bytes()); err != nil {
			return fmt.Errorf("shale: 写 WAL 失败: %w", err)
		}
		if db.opts.SyncWAL {
			// fsync 之后才敢说"数据落盘了"，断电也不丢。
			// 代价是慢一个数量级，所以默认关闭。
			if err := db.walFile.Sync(); err != nil {
				return fmt.Errorf("shale: WAL fsync 失败: %w", err)
			}
		}
	}

	// ② 再写内存。到这一步就算进程立刻崩溃，重启也能从日志恢复。
	var i uint64
	if err := b.Iterate(func(kind ikey.Kind, key, value []byte) error {
		db.mem.Add(base+i, kind, key, value)
		i++
		return nil
	}); err != nil {
		// 走到这里说明 batch 在校验通过之后又出了问题，属于内部错误。
		// MemTable 可能已经写进去了一部分 —— M2 有了 WAL 之后，
		// 这种情况会由"WAL 里没有这条记录"来兜底保证原子性。
		return err
	}
	db.seq = base + i - 1
	db.vs.SetLastSequence(db.seq)
	db.userBytesWritten += int64(b.Size())
	db.diskBytesWritten += int64(b.Size())

	// MemTable 写满了就落盘，腾出内存、同时让 WAL 得以被截断。
	//
	// 这里是【同步】刷的：写入会被阻塞到 flush 完成。
	// 真实引擎会交给后台 goroutine 并新建一个 MemTable 立刻接收写入，
	// 本项目留到 M9 —— 先把正确性做对。
	return db.maybeFlush()
}

// Flush 强制把当前 MemTable 刷成 SSTable。主要用于测试和调试。
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	return db.flushLocked()
}

// CompactAll 手动触发一次全量 compaction，把所有层合并干净。
// 这个操作很重，只适合在测试或维护窗口调用。
func (db *DB) CompactAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	// 先把内存里的刷下去，再合并 —— 否则 MemTable 里的数据参与不进来
	if err := db.flushLocked(); err != nil {
		return err
	}
	return db.compactAllLocked()
}

// Stats 返回当前的内部状态，用于观察 LSM 的运行情况。
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	st := Stats{
		MemTableSize:     db.mem.Size(),
		UserBytesWritten: db.userBytesWritten,
		DiskBytesWritten: db.diskBytesWritten,
		CompactionCount:  db.compactionCount,
	}
	v := db.vs.Current()
	for level := 0; level < version.MaxLevels; level++ {
		if v.NumFiles(level) == 0 && level > 0 {
			continue
		}
		st.Levels = append(st.Levels, LevelStats{
			Level:    level,
			NumFiles: v.NumFiles(level),
			Size:     v.LevelSize(level),
			MaxBytes: db.opts.LevelMaxBytes(level),
		})
	}
	return st
}

// ── 内部辅助 ────────────────────────────────────────────────

func validateKey(key []byte) error {
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: key %d 字节超过上限 %d", ErrKeyTooLarge, len(key), MaxKeySize)
	}
	return nil
}

func checkWritable(dir string) error {
	probe := filepath.Join(dir, ".shale-write-probe")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("shale: 目录不可写: %w", err)
	}
	f.Close()
	os.Remove(probe)
	return nil
}
