package shale

import (
	"bytes"
	"fmt"
	"os"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/iterator"
	"github.com/leyouhong/shale/internal/sstable"
	"github.com/leyouhong/shale/internal/version"
)

// 这个文件实现 compaction —— LSM 的后台整理。
//
// 对应 LSM 原理的：第 6~7 步（把文件合并起来）
//
// # 不整理会怎样
//
// 到 M5 为止，SSTable 只增不减。跑一会儿就变成这样：
//
//	598 个存活 key  →  散落在 136 个文件里
//
// 一次 Get 最坏要问遍所有文件，一次扫描要同时打开所有文件。
// 而且被覆盖的旧版本、删除留下的墓碑，全都还占着磁盘。
//
// # 怎么整理
//
// 把多个文件【归并成新文件】，顺便丢掉垃圾：
//
//	同一个 key 只保留最新版本，旧版本直接扔
//	墓碑推到最底层时可以连同它自己一起扔
//
// 因为每个文件内部都有序，归并就是多路归并排序 —— 全程顺序读、顺序写。
//
// # 为什么不用管"正在读的迭代器"
//
// 这是个容易担心的点：compaction 丢弃旧版本时，会不会把某个
// 长时间运行的迭代器需要的数据弄没了？
//
// 不会。因为 compaction 【从不修改现有文件】，只是生成新文件、
// 然后在新 Version 里把旧文件标记为已删除。
// 持有旧 Version 的迭代器读的还是旧文件，内容一个字节都没变；
// 而 M4 的引用计数保证了这些文件在最后一个读者离开前不会真正删盘。
//
// 所以本项目不需要 LevelDB 那套 smallest_snapshot 机制 ——
// 「不可变文件 + 版本引用计数」已经把这件事解决了。

// compaction 描述一次待执行的合并任务。
type compaction struct {
	// level 是输入的起始层，outputLevel 通常是 level+1。
	level       int
	outputLevel int

	// inputs[0] 是 level 层要参与的文件，inputs[1] 是 outputLevel 层的。
	inputs [2][]*version.FileMeta

	// isBottom 表示 outputLevel 之下再没有数据了。
	//
	// 只有此时才能丢弃墓碑 —— 否则更底层可能还藏着同一个 key 的旧数据，
	// 墓碑一没，那些数据就"复活"了。
	isBottom bool
}

// totalFiles 返回参与本次合并的文件总数。
func (c *compaction) totalFiles() int { return len(c.inputs[0]) + len(c.inputs[1]) }

// pickCompaction 挑选下一个要做的合并任务，没有就返回 nil。
//
// M6 用最笨的策略：L0 攒够文件就把【L0 全部 + L1 全部】合并成新的 L1。
// 好处是简单、且合并完 L1 天然满足"层内不重叠"；
// 坏处是每次都要重写整个 L1，数据一多就吃不消。
// M7 会换成真正的 Leveled：只挑一个文件，带上下层与它重叠的那几个。
func (db *DB) pickCompaction() *compaction {
	v := db.vs.Current()

	if v.NumFiles(0) < db.opts.L0CompactionTrigger {
		return nil
	}

	c := &compaction{level: 0, outputLevel: 1}
	c.inputs[0] = append([]*version.FileMeta(nil), v.Files(0)...)
	c.inputs[1] = append([]*version.FileMeta(nil), v.Files(1)...)

	// 输出层以下还有数据吗？没有的话就能安全地扔掉墓碑。
	c.isBottom = true
	for level := c.outputLevel + 1; level < version.MaxLevels; level++ {
		if v.NumFiles(level) > 0 {
			c.isBottom = false
			break
		}
	}
	return c
}

// maybeCompact 在需要时执行 compaction，直到没有任务为止。
// 调用方必须已持有写锁。
func (db *DB) maybeCompact() error {
	for {
		c := db.pickCompaction()
		if c == nil {
			return nil
		}
		if err := db.doCompaction(c); err != nil {
			return err
		}
	}
}

// doCompaction 执行一次合并。调用方必须已持有写锁。
func (db *DB) doCompaction(c *compaction) error {
	if c.totalFiles() == 0 {
		return nil
	}

	// ① 为所有输入文件建迭代器，归并成一个有序流
	var sources []iterator.Iterator
	for _, group := range c.inputs {
		for _, f := range group {
			r, err := db.table(f.Num)
			if err != nil {
				return err
			}
			sources = append(sources, r.NewIterator())
		}
	}
	merged := iterator.NewMergingIterator(sources...)
	defer merged.Close()

	// ② 边遍历边写出新文件
	out := &compactionOutput{db: db, level: c.outputLevel}
	defer out.abort() // 中途出错时清理半成品

	var lastKey []byte
	haveLast := false
	var dropped, kept int

	for merged.SeekToFirst(); merged.Valid(); merged.Next() {
		ik := merged.Key()
		uk := ikey.UserKey(ik)

		isNewKey := !haveLast || !bytes.Equal(uk, lastKey)

		if !isNewKey {
			// 同一个 key 的更旧版本 —— 上面已经保留过最新的了，这条是垃圾。
			//
			// 敢直接扔是因为：归并输出里同 key 按 seq 降序，
			// 第一条就是最新版本；而正在读的迭代器读的是旧文件，不受影响。
			dropped++
			continue
		}

		lastKey = append(lastKey[:0], uk...)
		haveLast = true

		if ikey.GetKind(ik) == ikey.KindDelete && c.isBottom {
			// 墓碑走到了最底层：下面再没有更老的数据了，
			// 它的使命完成，连自己一起消失。
			//
			// 反过来说，不是最底层时【绝不能】扔 —— 下层可能还有
			// 同一个 key 的旧值，墓碑一没那些值就复活了。
			dropped++
			continue
		}

		// 只在 key 边界处切换输出文件：
		// 同一个 user key 的所有版本必须待在同一个文件里，
		// 否则同层的两个文件会有重叠的 key 范围，破坏分层的前提。
		if out.shouldSplit() {
			if err := out.finish(); err != nil {
				return err
			}
		}
		if err := out.add(ik, merged.Value()); err != nil {
			return err
		}
		kept++
	}
	if err := merged.Error(); err != nil {
		return err
	}
	if err := out.finish(); err != nil {
		return err
	}

	// ③ 一条 VersionEdit 里同时记录"删旧的"和"加新的"，
	//    保证元数据的切换是原子的：要么全生效，要么全不生效。
	edit := &version.VersionEdit{}
	for level, group := range c.inputs {
		lv := c.level
		if level == 1 {
			lv = c.outputLevel
		}
		for _, f := range group {
			edit.DeleteFile(lv, f.Num)
		}
	}
	for _, m := range out.files {
		edit.AddFile(c.outputLevel, m)
	}
	if err := db.vs.LogAndApply(edit); err != nil {
		return err
	}
	out.committed = true

	db.compactionCount++
	db.droppedEntries += int64(dropped)

	// ④ 旧文件现在可以回收了 —— 但只有当没有迭代器还在用它们的时候。
	//    cleanupObsoleteFiles 会问 LiveFiles()，那里考虑了所有存活的 Version。
	return db.cleanupObsoleteFiles()
}

// compactionOutput 管理 compaction 产生的一批输出文件。
type compactionOutput struct {
	db    *DB
	level int

	file    *os.File
	writer  *sstable.Writer
	num     uint64
	maxSeq  uint64
	tmpPath string

	files     []version.FileMeta
	committed bool
}

// shouldSplit 判断当前文件是否已经够大、该切一个新的了。
func (o *compactionOutput) shouldSplit() bool {
	return o.writer != nil && o.writer.FileSize() >= o.db.opts.SSTableSize
}

// add 写入一条记录，必要时先开一个新文件。
func (o *compactionOutput) add(ik, value []byte) error {
	if o.writer == nil {
		if err := o.open(); err != nil {
			return err
		}
	}
	if s := ikey.Seq(ik); s > o.maxSeq {
		o.maxSeq = s
	}
	return o.writer.Add(ik, value)
}

func (o *compactionOutput) open() error {
	o.num = o.db.vs.NextFileNum()
	o.tmpPath = o.db.sstPath(o.num) + ".tmp"
	f, err := os.Create(o.tmpPath)
	if err != nil {
		return fmt.Errorf("shale: compaction 创建文件失败: %w", err)
	}
	o.file = f
	o.writer = sstable.NewWriter(f, o.db.opts.BlockSize)
	o.maxSeq = 0
	return nil
}

// finish 收尾当前输出文件：落盘、改名、登记元信息。
func (o *compactionOutput) finish() error {
	if o.writer == nil {
		return nil
	}
	if err := o.writer.Finish(); err != nil {
		return err
	}
	if err := o.file.Sync(); err != nil {
		return err
	}
	if err := o.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(o.tmpPath, o.db.sstPath(o.num)); err != nil {
		return fmt.Errorf("shale: compaction 发布文件失败: %w", err)
	}

	o.files = append(o.files, version.FileMeta{
		Num:      o.num,
		Size:     o.writer.FileSize(),
		Smallest: append([]byte(nil), o.writer.FirstKey()...),
		Largest:  append([]byte(nil), o.writer.LastKey()...),
		MaxSeq:   o.maxSeq,
	})
	o.db.diskBytesWritten += o.writer.FileSize()

	o.file, o.writer, o.tmpPath = nil, nil, ""
	return nil
}

// abort 在出错时清理已经产生的文件。
//
// 这些文件还没被 Manifest 登记，所以只是孤儿，删掉即可 ——
// 数据库的状态完全没被影响，就像这次 compaction 从没发生过。
func (o *compactionOutput) abort() {
	if o.committed {
		return
	}
	if o.file != nil {
		o.file.Close()
		os.Remove(o.tmpPath)
	}
	for _, m := range o.files {
		os.Remove(o.db.sstPath(m.Num))
	}
}

// compactAllLocked 反复合并，直到所有数据都沉到最底层的一个层里。
// 调用方必须已持有写锁。
//
// 这个操作很重（要重写全部数据），只适合测试或维护窗口。
func (db *DB) compactAllLocked() error {
	// 先按正常策略合并到没有任务为止
	if err := db.maybeCompact(); err != nil {
		return err
	}

	// 再把残留在 L0 的少量文件也并下去 —— 正常策略要求攒够
	// L0CompactionTrigger 个才动手，手动全量合并则不管数量。
	v := db.vs.Current()
	if v.NumFiles(0) == 0 {
		return nil
	}
	c := &compaction{level: 0, outputLevel: 1, isBottom: true}
	c.inputs[0] = append([]*version.FileMeta(nil), v.Files(0)...)
	c.inputs[1] = append([]*version.FileMeta(nil), v.Files(1)...)
	for level := 2; level < version.MaxLevels; level++ {
		if v.NumFiles(level) > 0 {
			c.isBottom = false
			break
		}
	}
	return db.doCompaction(c)
}
