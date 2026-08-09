package shale

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/memtable"
	"github.com/leyouhong/shale/internal/sstable"
	"github.com/leyouhong/shale/internal/version"
)

// 这个文件负责把 MemTable 落盘成 SSTable，以及管理已打开的文件句柄。
//
// M4 起，「有哪些文件」这件事不再靠扫目录，而是问 VersionSet ——
// 目录里的 .sst 只是"物理存在"，Manifest 说了才算"逻辑生效"。

const sstSuffix = ".sst"

// sstPath 返回编号为 num 的 SSTable 文件路径。
func (db *DB) sstPath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d%s", num, sstSuffix))
}

// tableCache 按文件编号缓存已打开的 SSTable Reader。
//
// 打开一个 Reader 要读 Footer、读 Index Block、扫描一遍统计边界，
// 不能每次查询都来一遍。
type tableCache map[uint64]*sstable.Reader

// table 返回某个文件的 Reader，必要时打开它。
//
// 这是【读路径上唯一会写共享状态】的地方（懒加载往 map 里塞句柄），
// 所以要单独用 tablesMu 保护 —— 读路径只持有 db.mu 的读锁，
// 挡不住多个 Get 同时写这个 map。
func (db *DB) table(num uint64) (*sstable.Reader, error) {
	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	if r, ok := db.tables[num]; ok {
		return r, nil
	}
	r, err := sstable.Open(db.sstPath(num))
	if err != nil {
		return nil, fmt.Errorf("shale: 打开 SSTable %06d 失败: %w", num, err)
	}
	r.SetCache(db.blockCache, num)
	db.tables[num] = r
	return r, nil
}

// dropTable 关闭并移除一个文件的句柄，顺便把它的统计收走。
func (db *DB) dropTable(num uint64) {
	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	r, ok := db.tables[num]
	if !ok {
		return
	}
	// 先把它累积的统计收走，再关掉 —— 否则这些数字就凭空消失了
	checks, skips := r.FilterStats()
	db.bloomChecks += checks
	db.bloomSkips += skips
	r.Close()
	delete(db.tables, num)
}

// cleanupObsoleteFiles 删除所有【不再被任何存活版本引用】的文件。
//
// 这就是引用计数机制的兑现处：
// compaction 说"这个文件不要了"，但只要还有迭代器持有引用它的旧版本，
// 文件就必须留着。等到引用归零，才在这里真正删掉。
//
// 同时也清理过期的 WAL：编号小于当前日志号的，其数据已经落进 SSTable 了。
func (db *DB) cleanupObsoleteFiles() error {
	if db.opts.ReadOnly {
		return nil
	}

	live := db.vs.LiveFiles()
	// 保留下界是"最老的、数据还没落盘的那个日志"，
	// 而不是"当前正在写的那个" —— 队列里的 immutable 还指望着它们呢。
	curLog := db.oldestLiveLogNum()

	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		num, kind, ok := parseFileName(name)
		if !ok {
			continue
		}
		keep := true
		switch kind {
		case fileSST:
			keep = live[num]
		case fileWAL:
			keep = num >= curLog
		}
		if keep {
			continue
		}
		// 从句柄缓存里摘掉再删文件。
		// 块缓存也要清 —— 否则那些块会一直占着容量，
		// 而且文件号将来可能被复用，留着会读到错误的数据。
		db.dropTable(num)
		if kind == fileSST {
			db.blockCache.EvictFile(num)
		}
		if err := os.Remove(filepath.Join(db.dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("shale: 删除废弃文件 %s 失败: %w", name, err)
		}
	}
	return nil
}

// cleanupTempFiles 删除上次崩溃留下的半成品 SSTable。
//
// 写 SSTable 时先写 .tmp，完成后再 rename —— rename 在同一个文件系统上
// 是【原子】的，所以目录里要么没有这个文件，要么是一个完整的文件，
// 绝不会出现"写了一半的 .sst"。留下的 .tmp 直接删掉就好。
func cleanupTempFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

type fileKind int

const (
	fileOther fileKind = iota
	fileSST
	fileWAL
)

// parseFileName 从文件名解析出编号和类型。
func parseFileName(name string) (uint64, fileKind, bool) {
	var suffix string
	var kind fileKind
	switch {
	case strings.HasSuffix(name, sstSuffix):
		suffix, kind = sstSuffix, fileSST
	case strings.HasSuffix(name, walSuffix):
		suffix, kind = walSuffix, fileWAL
	default:
		return 0, fileOther, false
	}
	base := strings.TrimSuffix(name, suffix)
	var num uint64
	if _, err := fmt.Sscanf(base, "%d", &num); err != nil {
		return 0, fileOther, false
	}
	return num, kind, true
}

// writeMemTableToSST 把一个（已冻结的）MemTable 写成 SSTable 并登记进 Manifest。
// 调用方必须持有 db.mu 写锁。
//
// 步骤顺序决定了崩溃安全性：
//
//	① 写成 .tmp 文件并 fsync
//	② rename 成正式的 .sst（原子操作）
//	③ 把这次变更写进 Manifest —— 到这一步文件才算「生效」
//
// 第 ③ 步之前崩溃，新 .sst 因为没被 Manifest 登记而被当作孤儿清掉，
// 数据仍在 WAL 里；第 ③ 步之后崩溃，数据已经在生效的 SSTable 里了。
// 两种情况都不丢数据。
func (db *DB) writeMemTableToSST(mem *memtable.MemTable) error {
	if mem.Empty() {
		return nil
	}

	fileNum := db.vs.NextFileNum()
	tmpPath := db.sstPath(fileNum) + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("shale: 创建 SSTable 失败: %w", err)
	}

	w := sstable.NewWriterWithBloom(f, db.opts.BlockSize, db.opts.BloomBitsPerKey)
	it := mem.NewIterator()
	var maxSeq uint64
	for it.SeekToFirst(); it.Valid(); it.Next() {
		// 原样写出【所有】记录，包括墓碑和被覆盖的旧版本。
		//
		// 这里绝不能"顺手清理一下"：更老的 SSTable 里可能还有同一个 key 的
		// 更旧版本，此刻丢掉墓碑会让那些旧数据复活。
		// 判断什么能真删是 compaction 的职责（它能看到所有层）。
		if err := w.Add(it.Key(), it.Value()); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("shale: 写 SSTable 失败: %w", err)
		}
		if s := it.Seq(); s > maxSeq {
			maxSeq = s
		}
	}
	if err := w.Finish(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("shale: 写 SSTable 失败: %w", err)
	}
	// fsync：必须确认数据真的落盘了，才敢在后面丢弃对应的 WAL
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("shale: SSTable fsync 失败: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	meta := version.FileMeta{
		Num:      fileNum,
		Size:     w.FileSize(),
		Smallest: append([]byte(nil), w.FirstKey()...),
		Largest:  append([]byte(nil), w.LastKey()...),
		MaxSeq:   maxSeq,
	}

	if err := os.Rename(tmpPath, db.sstPath(fileNum)); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("shale: 发布 SSTable 失败: %w", err)
	}

	edit := &version.VersionEdit{}
	edit.AddFile(0, meta) // flush 出来的文件总是进 L0
	edit.SetLogNumber(db.oldestLiveLogNum())
	edit.SetLastSequence(db.seq)
	if err := db.vs.LogAndApply(edit); err != nil {
		os.Remove(db.sstPath(fileNum))
		return err
	}

	db.flushCount++
	db.diskBytesWritten += meta.Size
	return nil
}

// oldestLiveLogNum 返回还不能删除的最小 WAL 编号。
//
// 队列里最老的那个 immutable 对应的日志，就是必须保留的下界 ——
// 它的数据还没落盘，日志是唯一的副本。队列空了就只需保留当前正在写的那个。
func (db *DB) oldestLiveLogNum() uint64 {
	if len(db.imm) > 0 {
		return db.imm[0].logNum
	}
	return db.frozenLogNum
}

// getFromTables 按【从新到旧】的顺序在所有 SSTable 里查找。
//
// 这个顺序不能反：新文件里的记录（包括墓碑）会覆盖旧文件里的同名 key，
// 先问旧的就会读到已经被改掉或删掉的值。
//
// L0 的文件之间 key 范围会重叠（都是 MemTable 直接刷下来的），
// 所以必须逐个问；编号越大越新，因此要倒着遍历。
func (db *DB) getFromTables(v *version.Version, key []byte, snapshot uint64) ([]byte, ikey.Lookup, error) {
	for level := 0; level < version.MaxLevels; level++ {
		files := v.Files(level)
		if len(files) == 0 {
			continue
		}
		if level == 0 {
			for i := len(files) - 1; i >= 0; i-- {
				val, res, err := db.lookupFile(files[i], key, snapshot)
				if err != nil || res != ikey.NotFound {
					return val, res, err
				}
			}
			continue
		}
		// L1 及以下：层内不重叠且按 key 排好序，
		// 所以一个 key 最多落在【一个】文件里，可以二分定位。
		//
		// 这是分层带来的读性能收益 —— 从"遍历该层所有文件"
		// 变成 O(log n) 次比较、只读一个文件。
		// 一层有几千个文件时，差距是决定性的。
		f := v.FindFile(level, key)
		if f == nil {
			continue // 这一层没有能覆盖它的文件
		}
		val, res, err := db.lookupFile(f, key, snapshot)
		if err != nil || res != ikey.NotFound {
			return val, res, err
		}
	}
	return nil, ikey.NotFound, nil
}

func (db *DB) lookupFile(f *version.FileMeta, key []byte, snapshot uint64) ([]byte, ikey.Lookup, error) {
	r, err := db.table(f.Num)
	if err != nil {
		return nil, ikey.NotFound, err
	}
	return r.Get(key, snapshot)
}

// closeTables 关闭所有打开的 SSTable 句柄。
func (db *DB) closeTables() error {
	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	var firstErr error
	for _, r := range db.tables {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.tables = make(tableCache)
	return firstErr
}
