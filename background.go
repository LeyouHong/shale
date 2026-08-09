package shale

import (
	"fmt"
	"sync"

	"github.com/leyouhong/shale/internal/memtable"
)

// 这个文件把 flush 和 compaction 挪到后台执行。
//
// # M8 之前的问题
//
// flush 和 compaction 都是【同步】做的：MemTable 一写满，
// 触发它的那次 Put 就得原地等着把几 MB 数据写完盘、
// 甚至等着一次 compaction 重写几十 MB —— 那次写入的延迟会突然跳到几百毫秒。
//
// 真实引擎的做法是：前台只管把数据放进内存就返回，
// 落盘和整理交给后台线程。
//
// # 冻结（Immutable MemTable）
//
// 关键在于 MemTable 写满时【不能原地刷】，而是：
//
//	① 把它冻结成 immutable，挂到待刷队列
//	② 立刻新建一个空 MemTable 接收后续写入   ← 前台不用等
//	③ 后台慢慢把 immutable 刷成 SSTable
//
// 查询时要同时看这两个：当前 MemTable（最新）→ immutable（次新）→ SSTable。
// 冻结之后的 MemTable 再也不会被写入，所以后台读它是安全的。
//
// # 什么时候还是得等
//
// 后台不是万能的。写入太快、后台跟不上时必须踩刹车，
// 否则内存会无限增长：
//
//	immutable 堆积到 MaxMemTables      → 等 flush 腾出位置
//	L0 文件数到 L0StopWritesTrigger    → 完全停写
//
// 这就是 RocksDB 里著名的 Write Stall。

// bgState 管理后台任务的生命周期。
type bgState struct {
	mu   sync.Mutex
	cond *sync.Cond

	// running 表示后台 goroutine 是否正在干活。
	running bool
	// closing 表示 DB 正在关闭，后台干完手头的活就该退出。
	closing bool
	// err 记录后台任务遇到的错误，会在下一次前台写入时抛出来。
	err error
}

func newBGState() *bgState {
	s := &bgState{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// maybeScheduleBG 在有活可干时叫醒后台 goroutine。
// 调用方必须持有 db.mu 写锁。
func (db *DB) maybeScheduleBG() {
	if db.opts.ReadOnly {
		return
	}

	db.bg.mu.Lock()
	defer db.bg.mu.Unlock()

	if db.bg.running || db.bg.closing || db.bg.err != nil {
		return // 已经在跑，或者不该再跑了
	}
	if len(db.imm) == 0 && db.pickCompaction() == nil {
		return // 没活可干
	}

	db.bg.running = true
	go db.backgroundLoop()
}

// backgroundLoop 是后台工作循环：有活就干，干完就退出。
//
// # 锁顺序（重要）
//
// 全项目统一一条规则：**要同时用两把锁时，永远先 db.mu 再 bg.mu**。
//
// 第一版写反了 —— 后台在持有 bg.mu 的情况下去拿 db.mu 判断"还有活吗"，
// 而前台是持有 db.mu 再拿 bg.mu，两边一撞就死锁。
// 现在的写法是：先在 db.mu 保护下把"还有没有活"算出来，
// 【放开 db.mu 之后】再去碰 bg.mu，两把锁不再嵌套。
func (db *DB) backgroundLoop() {
	for {
		db.mu.Lock()
		err := db.doBackgroundWork()
		// 趁着还持有 db.mu，把"是否还有活"一并算出来 ——
		// 出了这个临界区就不能再读 db 的状态了。
		more := err == nil && (len(db.imm) > 0 || db.pickCompaction() != nil)
		db.mu.Unlock()

		db.bg.mu.Lock()
		if err != nil {
			db.bg.err = err
		}
		if !more || db.bg.closing {
			db.bg.running = false
			db.bg.cond.Broadcast() // 叫醒所有等着腾位置的写入者
			db.bg.mu.Unlock()
			return
		}
		// 还有活，继续下一轮；顺便叫醒可能已经能继续的写入者
		db.bg.cond.Broadcast()
		db.bg.mu.Unlock()
	}
}

// doBackgroundWork 干一件事：优先刷 immutable，其次做 compaction。
// 调用方必须持有 db.mu 写锁。
//
// 为什么先刷 immutable？因为它占着内存、拦着新的冻结，
// 而 compaction 只是让读慢一点，没那么急。
func (db *DB) doBackgroundWork() error {
	if len(db.imm) > 0 {
		return db.flushOneImmutable()
	}
	if c := db.pickCompaction(); c != nil {
		return db.doCompaction(c)
	}
	return nil
}

// freezeMemTable 把当前 MemTable 冻结，换上一个新的。
// 调用方必须持有 db.mu 写锁。
//
// 这是"前台不用等落盘"的关键一步：冻结只是换个指针，几乎不耗时。
func (db *DB) freezeMemTable() error {
	if db.mem.Empty() {
		return nil
	}

	// 新 WAL 要先就位，否则后续写入会落进即将被回收的日志里
	newLogNum := db.vs.NextFileNum()
	if err := db.closeWAL(); err != nil {
		return err
	}
	if err := db.openWAL(newLogNum, 0); err != nil {
		return err
	}

	db.imm = append(db.imm, &immTable{
		mem:    db.mem,
		logNum: db.frozenLogNum,
	})
	db.frozenLogNum = newLogNum
	db.mem = memtable.New()
	return nil
}

// immTable 是一个等待刷盘的 MemTable，连同它对应的 WAL 编号。
//
// 记着 logNum 是为了知道刷完之后哪个日志可以删 ——
// 只有这个 MemTable 的数据全部落盘了，它的日志才失去价值。
type immTable struct {
	mem    *memtable.MemTable
	logNum uint64
}

// makeRoomForWrite 在必要时腾出写入空间。
// 调用方必须持有 db.mu 写锁；函数内部可能临时释放锁去等待。
//
// 三种情况：
//
//	① MemTable 没满            → 直接返回，最常见的路径
//	② MemTable 满了但队列有空位 → 冻结它、叫醒后台，前台继续
//	③ 队列满了 / L0 堆积过多   → 只能等（Write Stall）
func (db *DB) makeRoomForWrite() error {
	for {
		if err := db.bgError(); err != nil {
			return err
		}

		if db.mem.Size() < db.opts.MemTableSize {
			return nil // ① 还装得下
		}

		if len(db.imm) >= db.opts.MaxMemTables-1 {
			// ③ 待刷队列满了 —— 后台没跟上，必须等
			db.stallCount++
			if err := db.waitForBackground(); err != nil {
				return err
			}
			continue
		}

		if v := db.vs.Current(); v.NumFiles(0) >= db.opts.L0StopWritesTrigger {
			// ③ L0 堆积过多，再写下去读性能会崩 —— 完全停写
			db.stallCount++
			db.maybeScheduleBG()
			if err := db.waitForBackground(); err != nil {
				return err
			}
			continue
		}

		// ② 冻结换新，前台立刻可以继续
		if err := db.freezeMemTable(); err != nil {
			return err
		}
		db.maybeScheduleBG()
		return nil
	}
}

// waitForBackground 释放锁并等后台干完一轮。
//
// 注意锁顺序：进来时持有 db.mu，必须【先放开它】再去拿 bg.mu ——
// 否则后台拿不到 db.mu、永远干不完，双方互等。
func (db *DB) waitForBackground() error {
	db.maybeScheduleBG()

	db.mu.Unlock()
	db.bg.mu.Lock()
	if db.bg.running {
		db.bg.cond.Wait()
	}
	err := db.bg.err
	closing := db.bg.closing
	db.bg.mu.Unlock()
	db.mu.Lock()

	if err != nil {
		return err
	}
	if closing {
		// 正在关闭，不会再有后台任务来腾位置了 ——
		// 继续等下去就是死循环，直接报错让调用方知道。
		return ErrClosed
	}
	return nil
}

// bgError 返回后台任务的错误（如果有）。
//
// 后台出错不能就地 panic，也不能悄悄咽掉 ——
// 把它记下来，在下一次前台写入时报出去，让调用方知道数据库已经不健康了。
func (db *DB) bgError() error {
	db.bg.mu.Lock()
	defer db.bg.mu.Unlock()
	if db.bg.err != nil {
		return fmt.Errorf("shale: 后台任务失败，数据库进入只读状态: %w", db.bg.err)
	}
	return nil
}

// flushOneImmutable 把队列里最老的一个 immutable 刷成 SSTable。
// 调用方必须持有 db.mu 写锁。
func (db *DB) flushOneImmutable() error {
	if len(db.imm) == 0 {
		return nil
	}
	im := db.imm[0]

	if err := db.writeMemTableToSST(im.mem); err != nil {
		return err
	}

	// 落盘成功后才能把它从队列里摘掉、才能删它的日志
	db.imm = db.imm[1:]
	return db.cleanupObsoleteFiles()
}

// waitForBackgroundIdle 等所有后台任务干完，供 Close 和测试使用。
func (db *DB) waitForBackgroundIdle() {
	db.bg.mu.Lock()
	for db.bg.running {
		db.bg.cond.Wait()
	}
	db.bg.mu.Unlock()
}
