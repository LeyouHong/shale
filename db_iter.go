package shale

import (
	"bytes"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/iterator"
	"github.com/leyouhong/shale/internal/memtable"
	"github.com/leyouhong/shale/internal/version"
)

// 这个文件实现对外的 Iterator。
//
// 底下的归并迭代器吐出的是【原始内部记录】—— 墓碑、旧版本、
// 超出快照的新记录，全都在里面。这一层负责把它们过滤成
// 「用户眼里的数据」：每个 key 一条、只有最新可见版本、删掉的不出现。

// dbIterator 是 Iterator 接口的实现。
type dbIterator struct {
	inner    iterator.Iterator
	snapshot uint64

	// v 是创建时抓住的版本。持有它的引用，
	// 就保证了遍历期间那些 SSTable 文件不会被 compaction 删掉。
	v  *version.Version
	db *DB // 只为了 Close 时能拿锁释放引用

	// savedKey 是当前输出的用户 key。
	// 必须自己存一份 —— inner.Next() 之后它指向的内存就变了，
	// 而我们要用它来判断"下一条是不是同一个 key"。
	savedKey []byte
	value    []byte

	valid  bool
	closed bool
	err    error
}

// NewIterator 创建一个遍历全部数据的迭代器。
//
// 迭代器看到的是【创建那一刻】的快照：之后的写入、flush、compaction
// 都不会影响它。用完必须 Close，否则它引用的文件无法被回收。
func (db *DB) NewIterator() (Iterator, error) {
	// 这里必须用【写锁】而不是读锁：v.Ref() 会修改版本的引用计数、
	// 还可能改动 VersionSet 的存活集合。多个并发 NewIterator 同时
	// 改这些状态，会让存活集合错乱，进而导致 compaction 误删
	// 还有人在读的文件（表现为"打开 SSTable 失败：文件不存在"）。
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}

	v := db.vs.Current()
	v.Ref() // 拿住这一刻的文件集合，遍历期间它们不会被删

	var sources []iterator.Iterator

	// MemTable 要【复制一份快照】再遍历。
	//
	// 跳表不支持并发读写，而迭代器可能活很久，期间前台还在往
	// MemTable 里写。复制出来就把"边读边写"这个难题整个绕开了。
	// 代价是一次内存拷贝（MemTable 最多几 MB）。
	// M9 把跳表改成无锁之后，这里可以换成直接引用。
	sources = append(sources, snapshotMemTable(db.mem))

	// 等待刷盘的 immutable 也要算进来 —— 它们的数据还没进 SSTable，
	// 漏掉的话迭代器会看不到最近写入的一批数据。
	//
	// immutable 已经冻结、不会再被写入，本可以直接引用；
	// 这里仍然复制一份是为了和 mem 走同一条路径，少一处特殊情况。
	for _, im := range db.imm {
		sources = append(sources, snapshotMemTable(im.mem))
	}

	// 再把所有生效的 SSTable 加进来。
	// 顺序无所谓 —— 排序完全由内部 key 决定，新旧关系编码在 seq 里。
	for level := 0; level < version.MaxLevels; level++ {
		for _, f := range v.Files(level) {
			r, err := db.table(f.Num)
			if err != nil {
				v.Unref()
				return nil, err
			}
			sources = append(sources, r.NewIterator())
		}
	}

	it := &dbIterator{
		inner:    iterator.NewMergingIterator(sources...),
		snapshot: db.seq,
		v:        v,
		db:       db,
	}
	return it, nil
}

// snapshotMemTable 把一个 MemTable 的内容复制成只读迭代器。
func snapshotMemTable(m *memtable.MemTable) iterator.Iterator {
	entries := make([]iterator.Entry, 0, m.Count())
	it := m.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		entries = append(entries, iterator.Entry{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
	}
	return iterator.NewSliceIterator(entries)
}

// SeekToFirst 定位到最小的 key。
func (i *dbIterator) SeekToFirst() {
	if i.closed {
		return
	}
	i.inner.SeekToFirst()
	i.findNextVisible()
}

// Seek 定位到第一个 >= key 的位置。参数是【用户 key】。
func (i *dbIterator) Seek(key []byte) {
	if i.closed {
		return
	}
	// 用 snapshot 构造查找 key：直接落到"该 key 在快照时刻的版本"上，
	// 跳过那些更新的、快照看不见的版本。
	i.inner.Seek(ikey.MakeSeekKey(nil, key, i.snapshot))
	i.findNextVisible()
}

// Next 移动到下一个【用户 key】。
func (i *dbIterator) Next() {
	if i.closed || !i.valid {
		return
	}
	// 当前 key 的其余版本（更旧的）对用户不可见，全部跳过
	i.skipCurrentUserKey()
	i.findNextVisible()
}

// findNextVisible 往前找，直到停在一条用户可见的记录上。
//
// 三种要跳过的情况：
//
//	① seq > snapshot     —— 快照之后写的，这次遍历看不到
//	② 该 key 最新可见版本是墓碑 —— 整个 key 都要跳过
//	③ 同一个 key 的更旧版本 —— 已经输出过最新的了
func (i *dbIterator) findNextVisible() {
	for i.inner.Valid() {
		ik := i.inner.Key()

		if !ikey.Valid(ik) {
			i.err = ErrCorrupt
			i.valid = false
			return
		}

		if ikey.Seq(ik) > i.snapshot {
			// 比快照新，跳过这一条（同 key 可能还有更旧的、可见的版本）
			i.inner.Next()
			continue
		}

		// 走到这里，这就是该 user key 在快照下的【最新可见版本】——
		// 因为归并输出里同 key 按 seq 降序排列，第一个 seq <= snapshot 的就是它。
		i.savedKey = append(i.savedKey[:0], ikey.UserKey(ik)...)

		if ikey.GetKind(ik) == ikey.KindSet {
			i.value = i.inner.Value()
			i.valid = true
			return
		}

		// 是墓碑：这个 key 在快照时刻已经被删了。
		// 后面同 key 的旧版本全部作废，一起跳过。
		i.skipCurrentUserKey()
	}

	i.valid = false
	if err := i.inner.Error(); err != nil {
		i.err = err
	}
}

// skipCurrentUserKey 跳过 savedKey 的所有剩余版本。
func (i *dbIterator) skipCurrentUserKey() {
	for i.inner.Valid() && bytes.Equal(ikey.UserKey(i.inner.Key()), i.savedKey) {
		i.inner.Next()
	}
}

// Valid 返回当前是否停在有效位置。
func (i *dbIterator) Valid() bool { return i.valid && i.err == nil && !i.closed }

// Key 返回当前的用户 key。
//
// 返回的切片在下次 Next/Seek 之前有效，要保留请自行复制。
func (i *dbIterator) Key() []byte { return i.savedKey }

// Value 返回当前的值。同样只在下次移动之前有效。
func (i *dbIterator) Value() []byte { return i.value }

// Error 返回遍历中遇到的错误。
func (i *dbIterator) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.inner.Error()
}

// Close 释放迭代器。
//
// 必须调用：它持有的 Version 引用如果不释放，
// 那些本该被 compaction 删掉的文件会一直占着磁盘。
func (i *dbIterator) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	i.valid = false
	err := i.inner.Close()

	// Unref 会改动版本引用计数和 VersionSet 的存活集合，
	// 必须在 db.mu 写锁下做 —— 否则和后台 compaction 的
	// LiveFiles() 撞车，文件可能被误判为可删除。
	i.db.mu.Lock()
	i.v.Unref()
	i.db.mu.Unlock()
	return err
}

// 下面两个方法本项目暂不支持 —— 见 internal/iterator 里的说明：
// 反向遍历代价高（跳表无反向指针、SSTable 的块单向链接），且暂无场景需要。

// SeekToLast 暂不支持，调用后迭代器处于无效状态。
func (i *dbIterator) SeekToLast() {
	i.valid = false
	i.err = ErrNotImplemented
}

// Prev 暂不支持，调用后迭代器处于无效状态。
func (i *dbIterator) Prev() {
	i.valid = false
	i.err = ErrNotImplemented
}
