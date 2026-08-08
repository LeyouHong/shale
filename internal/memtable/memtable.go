// Package memtable 实现 MemTable —— 内存中的有序写缓冲。
//
// 对应 LSM 原理的：第 4 步（先在内存里排序，攒够了再一次性写出去）
//
// # 它解决什么问题
//
// LSM 想把数据写成【内部有序】的磁盘文件（SSTable），
// 但用户的写入是乱序来的：
//
//	Put("u5") → Put("u1") → Put("u9") → Put("u3")
//
// 如果每来一条就去文件里插到正确位置，就变成随机写了，失去了 LSM 的全部意义。
// 办法是：**先在内存里排好序，攒够一批再整个顺序写出去**。
// MemTable 就是这个"内存里的有序缓冲"。
//
// # 存的是内部 key
//
// MemTable 里存的不是用户 key，而是 ikey 包定义的内部 key
// （user key + seq + kind）。这带来两个性质：
//
//	· 同一个用户 key 的多次修改会作为【多条独立记录】共存，靠 seq 区分新旧
//	· 删除不是移除节点，而是插入一条 kind=Delete 的【墓碑】记录
//
// 所以 MemTable 只增不减 —— 这正是 LSM"从不原地修改"的体现。
//
// # 并发
//
// 本类型【不是】并发安全的，调用方必须保证互斥
// （DB 层目前用一把大锁；M9 再考虑放开）。
package memtable

import (
	"bytes"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/skiplist"
)

// MemTable 是内存中的有序写缓冲。
type MemTable struct {
	list *skiplist.SkipList
}

// New 创建一个空的 MemTable。
func New() *MemTable {
	// 注意传的是 ikey.Compare 而不是 bytes.Compare ——
	// 内部 key 的排序规则是「user key 升序 + seq 降序」，
	// 靠这个比较器，同一个 key 的最新版本会自动排在最前面。
	return &MemTable{list: skiplist.New(ikey.Compare)}
}

// Add 写入一条记录。
//
// kind 为 ikey.KindDelete 时 value 会被忽略（墓碑不带值）。
// userKey 和 value 都会被复制，调用方可以放心复用缓冲区。
func (m *MemTable) Add(seq uint64, kind ikey.Kind, userKey, value []byte) {
	// 用栈上数组做编码缓冲：绝大多数 key 都很短，能避免堆分配。
	//
	// 特意【不】在 MemTable 上挂一个共享的复用缓冲区 ——
	// 那样多个 goroutine 并发调用 Get 时会同时写它，产生数据竞争。
	var stack [72]byte
	ik := ikey.Encode(stack[:0], userKey, seq, kind)

	if kind == ikey.KindDelete {
		value = nil
	}
	m.list.Insert(ik, value)
}

// Get 查找一个用户 key 在 snapshot 时刻的值。
//
// snapshot 是一个序号：只考虑 seq <= snapshot 的版本，
// 用来实现"迭代器看到的是创建那一刻的快照"。想读最新数据就传当前的全局 seq。
//
// 三种返回见 ikey.Lookup：
//
//	ikey.NotFound —— 这个 MemTable 里没有该 key 的记录，调用方应【继续往下层找】
//	ikey.Found    —— 找到了值
//	ikey.Deleted  —— 找到了墓碑，该 key 已删除，【不要再往下找了】
//
// 返回的 value 指向内部存储，调用方要保存必须自己复制。
func (m *MemTable) Get(userKey []byte, snapshot uint64) ([]byte, ikey.Lookup) {
	var stack [72]byte
	seek := ikey.MakeSeekKey(stack[:0], userKey, snapshot)

	it := m.list.NewIterator()
	it.Seek(seek)

	if !it.Valid() {
		return nil, ikey.NotFound
	}

	// Seek 找到的是"第一个 >= seek key 的记录"。
	// 由于同一个 user key 内部按 seq 降序排，落点正好是
	// 【seq <= snapshot 的版本里最新的那个】—— 这就是要找的答案。
	//
	// 但落点也可能已经跨到下一个 user key 去了（说明这个 key 在快照时刻不存在），
	// 所以必须先确认 user key 真的匹配。
	found := it.Key()
	if !bytes.Equal(ikey.UserKey(found), userKey) {
		return nil, ikey.NotFound
	}

	if ikey.GetKind(found) == ikey.KindDelete {
		return nil, ikey.Deleted
	}
	return it.Value(), ikey.Found
}

// Size 返回估算的内存占用（字节）。DB 用它判断该不该刷盘。
//
// 注意它统计的是【所有版本】的总和：同一个 key 反复写 100 次，
// 这里就会累积 100 条记录的大小 —— 这是 LSM 空间放大的来源。
func (m *MemTable) Size() int64 { return m.list.Size() }

// Count 返回记录条数（含墓碑和被覆盖的旧版本）。
func (m *MemTable) Count() int { return m.list.Len() }

// Empty 判断是否一条记录都没有。
func (m *MemTable) Empty() bool { return m.list.Len() == 0 }

// ── 迭代器 ──────────────────────────────────────────────────

// Iterator 按内部 key 的顺序遍历 MemTable 里的【所有】记录 ——
// 包括墓碑和被覆盖的旧版本。
//
// 这正是 flush 到 SSTable 时需要的：不能在这一步就把旧版本丢掉，
// 因为更下层可能还有更老的数据，判断"能不能真删"是 compaction 的职责。
type Iterator struct {
	it *skiplist.Iterator
}

// NewIterator 创建一个遍历全部记录的迭代器。
func (m *MemTable) NewIterator() *Iterator {
	return &Iterator{it: m.list.NewIterator()}
}

// SeekToFirst 定位到最小的内部 key。
func (i *Iterator) SeekToFirst() { i.it.SeekToFirst() }

// SeekToLast 定位到最大的内部 key。
func (i *Iterator) SeekToLast() { i.it.SeekToLast() }

// Seek 定位到第一个 >= ik 的内部 key。参数是【内部 key】，不是用户 key。
func (i *Iterator) Seek(ik []byte) { i.it.Seek(ik) }

// SeekUserKey 定位到某个用户 key 在 snapshot 时刻的版本，是 Seek 的便利封装。
func (i *Iterator) SeekUserKey(userKey []byte, snapshot uint64) {
	var stack [72]byte
	i.it.Seek(ikey.MakeSeekKey(stack[:0], userKey, snapshot))
}

// Next 移动到下一条记录。
func (i *Iterator) Next() { i.it.Next() }

// Prev 移动到上一条记录。
func (i *Iterator) Prev() { i.it.Prev() }

// Valid 返回当前位置是否有效。
func (i *Iterator) Valid() bool { return i.it.Valid() }

// Key 返回当前的【内部 key】（含 seq 和 kind）。
func (i *Iterator) Key() []byte { return i.it.Key() }

// UserKey 返回当前记录的用户 key。
func (i *Iterator) UserKey() []byte { return ikey.UserKey(i.it.Key()) }

// Seq 返回当前记录的序号。
func (i *Iterator) Seq() uint64 { return ikey.Seq(i.it.Key()) }

// Kind 返回当前记录是写入还是墓碑。
func (i *Iterator) Kind() ikey.Kind { return ikey.GetKind(i.it.Key()) }

// Value 返回当前记录的值。墓碑的值为空。
func (i *Iterator) Value() []byte { return i.it.Value() }

// Error 永远返回 nil —— MemTable 全在内存里，遍历不会失败。
// 有这个方法是为了满足 internal/iterator 的 Iterator 接口，
// 好和 SSTable 的迭代器一起参与多路归并。
func (i *Iterator) Error() error { return nil }

// Close 无事可做，同样是为了满足接口。
func (i *Iterator) Close() error { return nil }
