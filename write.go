package shale

import (
	"fmt"
	"sync"

	"github.com/leyouhong/shale/internal/ikey"
)

// 这个文件实现 group commit（批量提交）—— 写入路径的核心优化。
//
// # 要解决的问题
//
// 写路径上有三处天然必须串行：
//
//	① 分配 seq     —— 它决定版本先后，不能乱序
//	② 追加 WAL     —— 一个文件顺序写，并发会交错成乱码
//	③ 冻结 MemTable —— 换指针的瞬间不能有人在写旧的
//
// 所以写入没法像读那样简单并发。之前的做法是一把大锁，
// 结果是并发写反而【更慢】（多了锁竞争，实测 12174 → 16278 ns/op）。
//
// 真正致命的是 fsync：开启 SyncWAL 后每次写都要等一次磁盘往返，
// 实测掉到 465 ops/s —— 慢了 567 倍。
//
// # 思路：一次 fsync 服务一群人
//
// 并发的写入者不各自排队挨个写，而是攒成一批，
// 推举队首那个当 leader，由它代表所有人写一次：
//
//	┌────────────────────────────────────────────────┐
//	│  写入队列                                        │
//	│  [batch A] ← leader   [batch B]   [batch C]     │
//	└──────┬─────────────────────────────────────────┘
//	       │ leader 把 A+B+C 合并成一个大 batch
//	       ▼
//	  ① 一次写 WAL（【只 fsync 一次】）
//	  ② 一次写 MemTable
//	  ③ 唤醒 B、C："你们的也写完了"
//
// fsync 是毫秒级的硬件往返，和数据量几乎无关。所以：
//
//	没有 group commit：100 个并发写 = 100 次 fsync
//	有 group commit：  100 个并发写 = 1 次 fsync
//
// 并发度越高，摊薄效果越好 —— 这是唯一能同时要"断电不丢"和"高吞吐"的办法。
//
// # 合并之所以几乎免费
//
// Batch 的二进制形式本身就是 WAL 记录，合并只是把记录区首尾相接、
// 把 count 加起来。合并后依然是一条合法的 WAL 记录，不需要任何转换。
// seq 由 leader 统一分配：整个大 batch 拿一个起始序号，
// 第 i 条记录是 base+i —— 各个 writer 的记录自然落在连续的区间里。

// writer 代表一个正在排队的写入者。
type writer struct {
	b   *Batch
	err error

	// done 表示"你的数据已经被 leader 代写完了"。
	done bool

	// cv 用于精确唤醒这一个 writer，底层锁是 db.mu。
	cv *sync.Cond
}

// maxGroupSize 是一批最多合并多少字节。
//
// 不能无限合并：批次太大时，队尾那些人要等的时间反而变长。
const maxGroupSize = 1 << 20 // 1MB

// smallBatchLimit 以下的写入被视为"小写入"。
//
// 小写入合并时额外限制增量，避免一个小请求被卷进一个巨大的批次里、
// 白等一次大 IO —— 它本来只需要写几十字节。
const smallBatchLimit = 128 << 10

// Write 原子地执行一批操作。
//
// 并发调用时会自动合并成组提交：只有队首的那个 goroutine 真正干活，
// 其余的等它代劳。返回时保证数据已经写进 WAL 和 MemTable。
func (db *DB) Write(b *Batch) error {
	if b == nil || b.Empty() {
		return nil
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	// 先校验再排队：非法的请求不该占用队列、更不该被合并进别人的批次
	if err := db.validateBatch(b); err != nil {
		return err
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}

	w := &writer{b: b, cv: sync.NewCond(&db.mu)}
	db.writers = append(db.writers, w)

	// 不是队首就睡着等 —— 要么被 leader 代写完（done），
	// 要么轮到自己当 leader
	for !w.done && db.writers[0] != w {
		w.cv.Wait()
	}
	if w.done {
		err := w.err
		db.mu.Unlock()
		return err
	}
	// 睡醒之后可能数据库已经关了
	if db.closed {
		db.removeWriter(w)
		db.mu.Unlock()
		return ErrClosed
	}

	// ── 从这里开始，我是 leader ──────────────────────────

	group, err := db.writeAsLeader()

	// 把这一批人全部唤醒，然后叫醒新的队首。
	//
	// 注意 group 是【写入时实际合并的那一批】，必须一路传下来 ——
	// 绝不能在这里重算（见 finishGroup 的说明）。
	db.finishGroup(group, err)
	db.mu.Unlock()
	return err
}

// writeAsLeader 由队首的 writer 执行：合并、落盘、写内存。
// 调用方必须持有 db.mu。
//
// # 为什么中途要放开锁
//
// 第一版把整个过程都放在 db.mu 里，结果【一次都没合并成】——
// 因为 leader 拿着锁做 fsync 时，其他写入者连队列都进不去，
// 全堵在 db.mu 上排队，而不是排在 writers 队列里。
//
// 所以最耗时的那段（写 WAL + fsync，毫秒级）必须在锁外做，
// 后来者才有机会入队、搭上下一班车。
//
// 放开锁期间靠两件事保证安全：
//
//	· 我还在队首，别人只会往队尾追加，不会抢走 leader 身份
//	· db.writing 标志挡住 Flush / CompactAll / Close，
//	  不让它们在这期间换掉 MemTable 或 WAL
//
// MemTable 的写入【不能】放在锁外 —— 本项目的跳表不支持一写多读
// （LevelDB 的跳表用 atomic 存指针，所以它连这步都能放出去）。
func (db *DB) writeAsLeader() ([]*writer, error) {
	// ① 先腾地方。这一步可能释放锁去等后台，
	//    但 leader 身份不会丢 —— 别人只会往队尾追加。
	if err := db.makeRoomForWrite(); err != nil {
		return []*writer{db.writers[0]}, err
	}

	// ② 把队列里能合的都合进来
	group, merged := db.buildGroup()

	// ③ 整个大 batch 拿一段连续的序号。
	//    必须【现在就把序号占掉】—— 待会儿要放开锁，
	//    不预留的话别人可能拿到重叠的区间。
	base := db.seq + 1
	count := uint64(merged.Count())
	merged.SetSeq(base)
	db.seq = base + count - 1
	db.vs.SetLastSequence(db.seq)

	// ④ 放开锁写日志。这是整个流程里最慢的一步（fsync 是毫秒级），
	//    也正是让后来者能排进队列的窗口。
	db.writing = true
	db.mu.Unlock()

	err := db.writeWAL(merged)

	db.mu.Lock()
	db.writing = false
	db.writingCond.Broadcast() // 放行可能在等的 Flush / Close

	if err != nil {
		return group, err
	}

	// ⑤ 回到锁内写内存。顺序不能反：先日志后内存，
	//    否则两步之间崩溃会出现"内存里有、日志里没有"，而内存马上也没了。
	var i uint64
	if err := merged.Iterate(func(kind ikey.Kind, key, value []byte) error {
		db.mem.Add(base+i, kind, key, value)
		i++
		return nil
	}); err != nil {
		return group, err
	}

	db.userBytesWritten += int64(merged.Size())
	db.diskBytesWritten += int64(merged.Size())

	db.groupCount++
	db.groupedWrites += int64(len(group))
	if n := int64(len(group)); n > db.maxGroupSeen {
		db.maxGroupSeen = n
	}
	return group, nil
}

// writeWAL 把合并后的 batch 追加到日志。在【不持有 db.mu】的情况下调用。
//
// 安全性由 db.writing 标志保证：它挡住了所有会切换 WAL 的操作。
func (db *DB) writeWAL(b *Batch) error {
	if db.wal == nil {
		return nil
	}
	if err := db.wal.Write(b.Bytes()); err != nil {
		return fmt.Errorf("shale: 写 WAL 失败: %w", err)
	}
	if db.opts.SyncWAL {
		// 这一次 fsync 服务了整批人 —— group commit 的全部意义所在
		if err := db.walFile.Sync(); err != nil {
			return fmt.Errorf("shale: WAL fsync 失败: %w", err)
		}
	}
	return nil
}

// waitForWriteInFlight 等待正在锁外进行的日志写入完成。
//
// 任何要切换 MemTable 或 WAL 的操作（Flush / CompactAll / Close）
// 都必须先调它 —— 否则会把 leader 正在写的那个 WAL 换掉，
// 数据就落到一个即将被删除的日志里了。
func (db *DB) waitForWriteInFlight() {
	for db.writing {
		db.writingCond.Wait()
	}
}

// buildGroup 从队首开始，把后面能合并的 writer 一起收进来。
//
// 返回被收进这一批的 writer 列表，以及合并后的 batch。
func (db *DB) buildGroup() ([]*writer, *Batch) {
	first := db.writers[0]
	merged := first.b
	size := merged.Size()

	// 小写入不要被拖进大批次：它本来只需要几十字节的 IO，
	// 却要等一个 1MB 的批次写完就不划算了。
	limit := maxGroupSize
	if size <= smallBatchLimit {
		limit = size + smallBatchLimit
	}

	group := []*writer{first}
	copied := false

	for _, w := range db.writers[1:] {
		if size+w.b.Size() > limit {
			break
		}
		if !copied {
			// 第一次合并时必须复制 —— 绝不能就地改调用方传进来的 Batch，
			// 它 Write 返回后可能还要被复用。
			m := NewBatch()
			m.Append(first.b)
			merged = m
			copied = true
		}
		merged.Append(w.b)
		size += w.b.Size()
		group = append(group, w)
	}
	return group, merged
}

// finishGroup 唤醒这一批被代写的 writer，并把队首交给下一个人。
// 调用方必须持有 db.mu。
//
// # 为什么 group 必须传进来、不能重算
//
// 第一版是在这里重新调 buildGroup() 算一遍的，注释还写着
// "队列前缀不变，两次结果一致" —— 这个推理是错的，代价是【数据丢失】。
//
// 前缀确实没变，但 leader 放开锁写日志的那段时间里，
// 队列尾部涨了一批新人。重算时 buildGroup 会贪心地把他们也收进来，
// 于是这些人被标记成"已完成"、从队列里移除，
// 可他们的数据【根本没被写过】—— 悄无声息地丢了。
//
// 所以必须把写入时真正合并的那一批原样传下来。
func (db *DB) finishGroup(group []*writer, err error) {
	if len(group) == 0 {
		return
	}
	leader := group[0]
	for _, w := range group {
		if w != leader {
			w.done = true
			w.err = err
			w.cv.Signal() // 精确唤醒这一个，不惊动其他人
		}
	}
	db.writers = db.writers[len(group):]

	// 队伍还没散就叫醒新的队首，让它接着当 leader
	if len(db.writers) > 0 {
		db.writers[0].cv.Signal()
	}
}

// removeWriter 把某个 writer 从队列里摘掉（只在异常路径用）。
func (db *DB) removeWriter(target *writer) {
	for i, w := range db.writers {
		if w == target {
			db.writers = append(db.writers[:i], db.writers[i+1:]...)
			return
		}
	}
}

// validateBatch 检查一批操作是否合法。不需要持有锁。
func (db *DB) validateBatch(b *Batch) error {
	return b.Iterate(func(_ ikey.Kind, key, value []byte) error {
		if err := validateKey(key); err != nil {
			return err
		}
		if int64(len(value)) > db.opts.MemTableSize {
			return fmt.Errorf("%w: value %d 字节超过 MemTableSize %d",
				ErrValueTooLarge, len(value), db.opts.MemTableSize)
		}
		return nil
	})
}

// drainWriters 在关闭时把还在排队的人叫醒并告诉它们数据库关了。
// 调用方必须持有 db.mu。
func (db *DB) drainWriters() {
	for _, w := range db.writers {
		w.done = true
		w.err = ErrClosed
		w.cv.Signal()
	}
	db.writers = nil
}
