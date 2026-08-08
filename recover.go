package shale

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/leyouhong/shale/internal/ikey"
	"github.com/leyouhong/shale/internal/wal"
)

// 这个文件负责崩溃恢复：启动时把 WAL 重放回 MemTable。
//
// 整个流程是 LSM「数据能活过重启」的全部秘密：
//
//	写入时：先把 batch 追加到 WAL，成功之后才写 MemTable
//	重启时：把 WAL 从头重放一遍，MemTable 就回来了
//
// 关键在于处理【崩溃留下的残片】：进程可能死在写入的任何一个字节中间，
// 所以日志尾部大概率是半条记录。恢复流程必须：
//
//	① 一路读到最后一条完整记录为止（wal.Reader 负责识别）
//	② 把文件截断到那个位置，切掉残片
//	③ 从截断处继续追加，后续写入才不会接在垃圾后面

// walSuffix 是 WAL 文件的扩展名，文件名形如 000001.log。
const walSuffix = ".log"

// walPath 返回编号为 num 的 WAL 文件路径。
func (db *DB) walPath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d%s", num, walSuffix))
}

// listWALs 列出目录里所有 WAL 文件，按编号从小到大排序。
//
// 编号顺序就是写入的时间顺序，重放必须按这个顺序来 ——
// 否则老数据会覆盖新数据。
func listWALs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), walSuffix) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), walSuffix)
		n, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue // 不认识的文件名，跳过
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

// recover 重放所有 WAL，重建 MemTable 和 seq。
//
// 返回后 db.wal 已经准备好接收新的写入。
func (db *DB) recover() error {
	nums, err := listWALs(db.dir)
	if err != nil {
		return fmt.Errorf("shale: 扫描 WAL 失败: %w", err)
	}

	var lastValidSize int64
	for i, num := range nums {
		size, err := db.replayWAL(db.walPath(num))
		if err != nil {
			return err
		}
		if i == len(nums)-1 {
			lastValidSize = size
		}
	}

	if db.opts.ReadOnly {
		return nil // 只读模式不写日志
	}

	if len(nums) == 0 {
		// 全新的数据库，开一个 1 号日志
		return db.openWAL(1, 0)
	}

	// 复用最后一个日志文件：先截掉崩溃留下的残片，再从有效末尾续写。
	//
	// 为什么不新开一个文件？因为此刻 MemTable 里的数据【还没落盘】
	// （M3 才有 SSTable），旧日志是它唯一的持久化副本，不能丢。
	// 截断 + 续写既保住了数据，又不会让文件越积越多。
	last := nums[len(nums)-1]
	if err := os.Truncate(db.walPath(last), lastValidSize); err != nil {
		return fmt.Errorf("shale: 截断 WAL 失败: %w", err)
	}
	return db.openWAL(last, lastValidSize)
}

// replayWAL 重放一个日志文件，返回其中最后一条完整记录结束的偏移。
func (db *DB) replayWAL(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("shale: 打开 WAL 失败: %w", err)
	}
	defer f.Close()

	r := wal.NewReader(f)
	b := NewBatch()

	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break // 正常读完，或者读到了崩溃留下的残片
		}
		if err != nil {
			// 内容真的损坏了（CRC 对不上）。
			//
			// 策略：停止重放，保留已经恢复的部分。
			// 继续往后读是危险的 —— 无法判断后面的内容是否可信，
			// 而丢弃全部又太浪费。这是数据库通行的做法。
			db.recoverWarning = fmt.Errorf("WAL %s 在偏移 %d 处损坏，只恢复了此前的数据: %w",
				filepath.Base(path), r.ValidSize(), err)
			break
		}

		if err := b.Load(rec); err != nil {
			db.recoverWarning = fmt.Errorf("WAL %s 中有无法解析的 batch，只恢复了此前的数据: %w",
				filepath.Base(path), err)
			break
		}

		// 重放时【原样沿用记录里的 seq】，绝不能重新分配 ——
		// seq 决定了同一个 key 的版本先后，改了顺序就全乱了。
		base := b.Seq()
		var i uint64
		if err := b.Iterate(func(kind ikey.Kind, key, value []byte) error {
			db.mem.Add(base+i, kind, key, value)
			i++
			return nil
		}); err != nil {
			return 0, fmt.Errorf("shale: 重放 batch 失败: %w", err)
		}

		if last := base + i - 1; last > db.seq {
			db.seq = last
		}
		db.recovered += int(i)
	}

	return r.ValidSize(), nil
}

// openWAL 打开（或创建）编号为 num 的日志文件，从 offset 处开始写。
func (db *DB) openWAL(num uint64, offset int64) error {
	f, err := os.OpenFile(db.walPath(num), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("shale: 打开 WAL 失败: %w", err)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("shale: 定位 WAL 失败: %w", err)
	}

	db.walFile = f
	db.walNum = num
	// 必须告诉 Writer 当前偏移 —— 它要据此算出在块内的位置，
	// 否则续写的第一条记录会跨错块边界，读的时候就散架了。
	db.wal = wal.NewWriterAt(f, offset)
	return nil
}

// closeWAL 关闭当前日志文件。
func (db *DB) closeWAL() error {
	if db.walFile == nil {
		return nil
	}
	// 关闭前 fsync 一次：正常关闭不该丢数据，
	// 哪怕 SyncWAL=false（那个选项管的是每次写入要不要 sync）。
	err := db.walFile.Sync()
	if cerr := db.walFile.Close(); err == nil {
		err = cerr
	}
	db.walFile = nil
	db.wal = nil
	return err
}
