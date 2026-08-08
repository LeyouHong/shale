package shale

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/memtable"
	"github.com/leyouhong/shale/internal/sstable"
)

// 这个文件负责把 MemTable 落盘成 SSTable。
//
// 这一步之前，所有数据都堆在内存里、WAL 只增不减；有了 flush 之后：
//
//	MemTable 写满 → 冻结 → 写成一个 SSTable 文件 → 丢弃它和对应的 WAL
//
// 于是内存占用和日志长度都被限制住了，数据库才真正能长期运行。

const sstSuffix = ".sst"

// sstPath 返回编号为 num 的 SSTable 文件路径。
func (db *DB) sstPath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d%s", num, sstSuffix))
}

// tableFile 是一个已打开的 SSTable 及其元信息。
type tableFile struct {
	num    uint64
	reader *sstable.Reader
}

// listSSTs 列出目录里所有 SSTable，按编号从小到大排序。
//
// 编号越大越新 —— 查找时必须【从新到旧】依次询问，
// 否则会读到已经被覆盖的旧值。
func listSSTs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), sstSuffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), sstSuffix), 10, 64)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// openTables 打开目录里所有 SSTable 文件。
func (db *DB) openTables() error {
	nums, err := listSSTs(db.dir)
	if err != nil {
		return fmt.Errorf("shale: 扫描 SSTable 失败: %w", err)
	}
	for _, num := range nums {
		r, err := sstable.Open(db.sstPath(num))
		if err != nil {
			return fmt.Errorf("shale: 打开 SSTable %06d 失败: %w", num, err)
		}
		db.tables = append(db.tables, &tableFile{num: num, reader: r})
		if num >= db.nextFileNum {
			db.nextFileNum = num + 1
		}
		// 从已落盘的数据里恢复全局序号。
		//
		// 这一步不能省：flush 之后对应的 WAL 就被删了，那批记录的 seq
		// 只剩 SSTable 里还有。少了它，重启后 seq 会归零，
		// 新写入反而会被当成比老数据更旧 —— 表现为"重启后数据全查不到"。
		if ms := r.MaxSeq(); ms > db.seq {
			db.seq = ms
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

// maybeFlush 在 MemTable 超出阈值时把它刷成 SSTable。
// 调用方必须已持有写锁。
func (db *DB) maybeFlush() error {
	if db.mem.Size() < db.opts.MemTableSize {
		return nil
	}
	return db.flushLocked()
}

// flushLocked 把当前 MemTable 写成一个 SSTable 文件。
// 调用方必须已持有写锁。
//
// 步骤的顺序是崩溃安全的关键：
//
//	① 先开一个新 WAL，让后续写入落到新日志里
//	② 把 MemTable 写成 .tmp 文件并 fsync
//	③ rename 成正式的 .sst（原子操作）
//	④ 这时才敢删掉旧 WAL —— 数据已经在 SSTable 里了
//
// 任何一步崩溃都不会丢数据：只要旧 WAL 还在，重启就能重放出来。
func (db *DB) flushLocked() error {
	if db.mem.Empty() {
		return nil
	}

	oldWALNum := db.walNum
	fileNum := db.nextFileNum
	db.nextFileNum++

	// ① 新 WAL 先就位。必须在写 SSTable 之前做，
	//    否则 flush 期间的新写入会落进即将被删除的旧日志里。
	if !db.opts.ReadOnly {
		if err := db.closeWAL(); err != nil {
			return err
		}
		if err := db.openWAL(db.nextFileNum, 0); err != nil {
			return err
		}
		db.nextFileNum++
	}

	// ② 写成临时文件
	tmpPath := db.sstPath(fileNum) + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("shale: 创建 SSTable 失败: %w", err)
	}

	w := sstable.NewWriter(f, db.opts.BlockSize)
	it := db.mem.NewIterator()
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
	}
	if err := w.Finish(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("shale: 写 SSTable 失败: %w", err)
	}
	// fsync：必须确认数据真的落盘了，才敢在第 ④ 步删掉 WAL
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("shale: SSTable fsync 失败: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	entries := w.EntryCount()
	size := w.FileSize()

	// ③ 原子地"发布"这个文件
	finalPath := db.sstPath(fileNum)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("shale: 发布 SSTable 失败: %w", err)
	}

	r, err := sstable.Open(finalPath)
	if err != nil {
		return fmt.Errorf("shale: 打开新建的 SSTable 失败: %w", err)
	}
	db.tables = append(db.tables, &tableFile{num: fileNum, reader: r})

	// ④ 数据已经安全落盘，旧 WAL 可以退休了
	if oldWALNum != 0 && !db.opts.ReadOnly {
		if err := os.Remove(db.walPath(oldWALNum)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("shale: 删除旧 WAL 失败: %w", err)
		}
	}

	db.mem = memtable.New()
	db.flushCount++
	db.diskBytesWritten += size

	_ = entries // 保留变量以便将来记录统计
	return nil
}

// getFromTables 按【从新到旧】的顺序在所有 SSTable 里查找。
//
// 这个顺序不能反：新文件里的记录（包括墓碑）会覆盖旧文件里的同名 key，
// 先问旧的就会读到已经被改掉或删掉的值。
func (db *DB) getFromTables(key []byte, snapshot uint64) ([]byte, ikey.Lookup, error) {
	for i := len(db.tables) - 1; i >= 0; i-- {
		v, res, err := db.tables[i].reader.Get(key, snapshot)
		if err != nil {
			return nil, ikey.NotFound, err
		}
		if res != ikey.NotFound {
			return v, res, nil // Found 或 Deleted 都是确定的答案，就此打住
		}
	}
	return nil, ikey.NotFound, nil
}

// closeTables 关闭所有打开的 SSTable。
func (db *DB) closeTables() error {
	var firstErr error
	for _, t := range db.tables {
		if err := t.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.tables = nil
	return firstErr
}
