// Package iterator 定义内部迭代器接口，并实现多路归并。
//
// 对应 LSM 原理的：第 6 步（从多个地方读数据）
//
// # 为什么范围扫描比点查难
//
// 点查可以"从新到旧问一遍，谁先给出确定答案就用谁的"，找到就能停。
// 范围扫描不行 —— 下一个 key 可能来自任何一个地方：
//
//	MemTable   a  c  f
//	L0-文件1   b  c  e
//	L1-文件7   a  d  g
//	           ↓
//	归并输出   a  a  b  c  c  d  e  f  g     ← 必须同时打开所有源
//
// 所以必须做【多路归并】：同时推进所有源，每次挑出 key 最小的那个。
//
// # 这一层不做过滤
//
// 本包的迭代器遍历的是【内部 key】，会原样吐出所有记录 ——
// 包括墓碑、包括同一个 key 的多个版本。
//
// 为什么不在这里就去重？因为有两个不同的消费者，需求正相反：
//
//	用户查询   只想看到每个 key 的最新可见版本，墓碑要跳过
//	compaction 必须看到全部原始记录，才能正确判断什么能丢弃
//
// 所以过滤逻辑放在更上层（db 层的用户迭代器 / compaction），
// 本包只负责"把多个有序源合并成一个有序流"这一件事。
package iterator

import (
	"github.com/leyouhong/shale/internal/ikey"
)

// Iterator 是内部迭代器，按内部 key 升序遍历。
//
// 注意只有正向遍历。反向遍历（Prev / SeekToLast）代价高得多 ——
// 跳表没有反向指针、SSTable 的块也是单向链的 —— 而且当前没有场景需要，
// 所以干脆不放进接口，免得出现"有这个方法但会 panic"的陷阱。
type Iterator interface {
	// SeekToFirst 定位到第一条记录。
	SeekToFirst()

	// Seek 定位到第一个 >= target 的记录。target 是内部 key。
	Seek(target []byte)

	// Next 前进一条。只在 Valid() 为 true 时调用。
	Next()

	// Valid 返回当前是否停在有效记录上。
	Valid() bool

	// Key 返回当前的内部 key。
	Key() []byte

	// Value 返回当前的值。
	Value() []byte

	// Error 返回遍历中遇到的错误。
	Error() error

	// Close 释放资源。
	Close() error
}

// MergingIterator 把多个有序的源合并成一个有序流。
//
// 归并的正确性依赖一个前提：每个源自身按内部 key 升序。
// MemTable（跳表）和 SSTable 都满足，因为它们用的是同一个 ikey.Compare。
//
// 输出顺序因此天然是「用户 key 升序 + 同 key 内 seq 降序」，
// 也就是说【同一个 key 的最新版本一定先出来】——
// 上层的去重逻辑正是靠这一点：拿到第一个就是答案，后面同 key 的全跳过。
type MergingIterator struct {
	iters []Iterator

	// current 是当前 key 最小的那个源的下标，-1 表示已经走完。
	current int

	err error
}

// NewMergingIterator 创建一个归并迭代器。
//
// 传入的迭代器所有权转移给它，Close 时会一并关闭。
// 源的先后顺序【不影响】结果 —— 排序完全由内部 key 决定，
// 新旧关系已经编码在 seq 里了。
func NewMergingIterator(iters ...Iterator) *MergingIterator {
	// 过滤掉 nil，调用方拼装源列表时会方便一些
	live := make([]Iterator, 0, len(iters))
	for _, it := range iters {
		if it != nil {
			live = append(live, it)
		}
	}
	return &MergingIterator{iters: live, current: -1}
}

// SeekToFirst 让所有源都定位到各自的第一条，然后挑出最小的。
func (m *MergingIterator) SeekToFirst() {
	for _, it := range m.iters {
		it.SeekToFirst()
	}
	m.findSmallest()
}

// Seek 让所有源都定位到 >= target 的第一条，然后挑出最小的。
func (m *MergingIterator) Seek(target []byte) {
	for _, it := range m.iters {
		it.Seek(target)
	}
	m.findSmallest()
}

// Next 前进一条。
//
// 只推进【当前输出的那个源】—— 其他源还停在各自的位置上，
// 它们的当前 key 都 >= 刚输出的 key，下一轮重新比较即可。
func (m *MergingIterator) Next() {
	if m.current < 0 {
		return
	}
	m.iters[m.current].Next()
	m.findSmallest()
}

// findSmallest 在所有源里挑出 key 最小的那个。
//
// 这里是线性扫描而不是最小堆。源的数量通常不多
// （1 个 MemTable + 几个 L0 文件 + 每层 1 个），
// 线性扫描的常数更小、代码也简单得多。
// 如果 L0 文件堆到几十个，才值得换成堆 —— 但那种情况本身
// 就说明 compaction 跟不上，该解决的是那个问题。
func (m *MergingIterator) findSmallest() {
	smallest := -1
	for i, it := range m.iters {
		if err := it.Error(); err != nil {
			m.err = err
			m.current = -1
			return
		}
		if !it.Valid() {
			continue
		}
		if smallest < 0 || ikey.Compare(it.Key(), m.iters[smallest].Key()) < 0 {
			smallest = i
		}
	}
	m.current = smallest
}

// Valid 返回当前是否有有效记录。
func (m *MergingIterator) Valid() bool {
	return m.err == nil && m.current >= 0
}

// Key 返回当前的内部 key。
func (m *MergingIterator) Key() []byte { return m.iters[m.current].Key() }

// Value 返回当前的值。
func (m *MergingIterator) Value() []byte { return m.iters[m.current].Value() }

// Error 返回遍历中遇到的错误。
func (m *MergingIterator) Error() error {
	if m.err != nil {
		return m.err
	}
	for _, it := range m.iters {
		if err := it.Error(); err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭所有源。
func (m *MergingIterator) Close() error {
	var firstErr error
	for _, it := range m.iters {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.iters = nil
	m.current = -1
	return firstErr
}

// ── 切片迭代器 ──────────────────────────────────────────────

// Entry 是一条内部记录。
type Entry struct {
	Key   []byte // 内部 key
	Value []byte
}

// SliceIterator 遍历一个已经排好序的切片。
//
// 用途之一是给 MemTable 做只读快照：跳表本身不支持并发读写，
// 而迭代器可能存活很久，期间前台还在写入。
// 把内容复制出来，就把"边读边写"这个难题绕开了。
type SliceIterator struct {
	entries []Entry
	pos     int
}

// NewSliceIterator 创建一个切片迭代器。entries 必须已按内部 key 升序排列。
func NewSliceIterator(entries []Entry) *SliceIterator {
	return &SliceIterator{entries: entries, pos: -1}
}

func (s *SliceIterator) SeekToFirst() { s.pos = 0 }

func (s *SliceIterator) Seek(target []byte) {
	// 二分找第一个 >= target 的
	lo, hi := 0, len(s.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if ikey.Compare(s.entries[mid].Key, target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	s.pos = lo
}

func (s *SliceIterator) Next() { s.pos++ }

func (s *SliceIterator) Valid() bool {
	return s.pos >= 0 && s.pos < len(s.entries)
}

func (s *SliceIterator) Key() []byte   { return s.entries[s.pos].Key }
func (s *SliceIterator) Value() []byte { return s.entries[s.pos].Value }
func (s *SliceIterator) Error() error  { return nil }
func (s *SliceIterator) Close() error  { s.entries = nil; return nil }
