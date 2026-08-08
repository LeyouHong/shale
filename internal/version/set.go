package version

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/leyouhong/shale/internal/wal"
)

// Manifest 相关的文件名。
const (
	// currentFile 是一个只有一行的小文件，内容是当前生效的 MANIFEST 文件名。
	//
	// 为什么要多这一层间接？因为切换 Manifest 必须是【原子】的：
	// 先把新 Manifest 完整写好，再用 rename 替换 CURRENT。
	// 任何时刻崩溃，CURRENT 要么指向旧的、要么指向新的，不会指向半成品。
	currentFile = "CURRENT"

	manifestPrefix = "MANIFEST-"
)

// VersionSet 管理当前生效的 Version，以及 Manifest 的读写。
//
// 它是元数据的唯一入口：想知道"现在有哪些文件"，只能问它。
type VersionSet struct {
	dir string

	// current 是当前生效的版本。
	current *Version

	// live 是所有还有人引用的版本。
	// compaction 删掉的文件，必须等到所有引用它的版本都消失才能真正删除。
	live map[*Version]bool

	nextFileNum uint64
	lastSeq     uint64
	logNum      uint64

	manifestFile *os.File
	manifest     *wal.Writer
	manifestNum  uint64
}

// NewVersionSet 创建一个空的 VersionSet（用于全新的数据库）。
func NewVersionSet(dir string) *VersionSet {
	vs := &VersionSet{
		dir:         dir,
		live:        make(map[*Version]bool),
		nextFileNum: 1,
	}
	v := newVersion(vs)
	vs.appendVersion(v)
	return vs
}

// Current 返回当前生效的版本。
//
// 调用方如果要长期持有（比如迭代器），必须先 Ref。
func (vs *VersionSet) Current() *Version { return vs.current }

// NextFileNum 分配一个新的文件编号。
func (vs *VersionSet) NextFileNum() uint64 {
	n := vs.nextFileNum
	vs.nextFileNum++
	return n
}

// PeekNextFileNum 查看下一个编号但不分配。
func (vs *VersionSet) PeekNextFileNum() uint64 { return vs.nextFileNum }

// LastSequence 返回全局最大序号。
func (vs *VersionSet) LastSequence() uint64 { return vs.lastSeq }

// SetLastSequence 更新全局最大序号。
func (vs *VersionSet) SetLastSequence(n uint64) { vs.lastSeq = n }

// LogNumber 返回当前 WAL 的编号。
func (vs *VersionSet) LogNumber() uint64 { return vs.logNum }

// appendVersion 把新版本设为当前版本。
func (vs *VersionSet) appendVersion(v *Version) {
	v.Ref() // VersionSet 自己持有一份引用
	if vs.current != nil {
		vs.current.Unref()
	}
	vs.current = v
	vs.live[v] = true
}

// removeVersion 在某个版本引用归零时把它从存活集合里摘掉。
func (vs *VersionSet) removeVersion(v *Version) {
	delete(vs.live, v)
}

// LogAndApply 应用一条变更：先写进 Manifest，再生成新的 Version。
//
// 顺序不能反 —— 和 WAL 一个道理：
// 必须先把变更记到磁盘上，内存里的状态才敢跟着变。
// 否则崩溃后内存状态消失，磁盘上却没有对应记录，两边就对不上了。
func (vs *VersionSet) LogAndApply(e *VersionEdit) error {
	// 补齐几个全局字段，让 Manifest 自带完整的恢复信息
	if !e.hasNextFile {
		e.SetNextFileNum(vs.nextFileNum)
	}
	if !e.hasLastSeq {
		e.SetLastSequence(vs.lastSeq)
	}
	if !e.hasLogNumber {
		e.SetLogNumber(vs.logNum)
	}

	if vs.manifest != nil {
		if err := vs.manifest.Write(e.Encode()); err != nil {
			return fmt.Errorf("version: 写 Manifest 失败: %w", err)
		}
		// fsync：元数据比数据本身更不能丢 ——
		// 数据文件还在但元数据说它不存在，等于数据丢了。
		if err := vs.manifestFile.Sync(); err != nil {
			return fmt.Errorf("version: Manifest fsync 失败: %w", err)
		}
	}

	v := vs.current.clone()
	if err := v.apply(e); err != nil {
		return err
	}

	vs.applyGlobals(e)
	vs.appendVersion(v)
	return nil
}

func (vs *VersionSet) applyGlobals(e *VersionEdit) {
	if e.hasNextFile && e.NextFileNum > vs.nextFileNum {
		vs.nextFileNum = e.NextFileNum
	}
	if e.hasLastSeq && e.LastSequence > vs.lastSeq {
		vs.lastSeq = e.LastSequence
	}
	if e.hasLogNumber {
		vs.logNum = e.LogNumber
	}
}

// Recover 从磁盘上的 CURRENT + MANIFEST 重建元数据。
//
// 返回 false 表示这是一个全新的数据库（没有 CURRENT 文件）。
func (vs *VersionSet) Recover() (bool, error) {
	name, err := os.ReadFile(filepath.Join(vs.dir, currentFile))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("version: 读 CURRENT 失败: %w", err)
	}

	manifestName := strings.TrimSpace(string(name))
	f, err := os.Open(filepath.Join(vs.dir, manifestName))
	if err != nil {
		return false, fmt.Errorf("version: 打开 Manifest %s 失败: %w", manifestName, err)
	}
	defer f.Close()

	// 从头重放所有 edit —— 和 WAL 重建 MemTable 完全相同的套路。
	v := newVersion(vs)
	r := wal.NewReader(f)
	count := 0
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Manifest 尾部损坏：保留已经重放出来的部分。
			// 这比直接拒绝启动好 —— 至少还能读到大部分数据。
			break
		}
		var e VersionEdit
		if err := e.Decode(rec); err != nil {
			return false, fmt.Errorf("version: 解析 Manifest 第 %d 条记录失败: %w", count, err)
		}
		if err := v.apply(&e); err != nil {
			return false, fmt.Errorf("version: 应用 Manifest 第 %d 条记录失败: %w", count, err)
		}
		vs.applyGlobals(&e)
		count++
	}

	if n, err := parseManifestNum(manifestName); err == nil {
		vs.manifestNum = n
		if n >= vs.nextFileNum {
			vs.nextFileNum = n + 1
		}
	}

	vs.appendVersion(v)
	return true, nil
}

// CreateManifest 写出一个【全新的 Manifest】，内容是当前版本的完整快照，
// 然后原子地把 CURRENT 指过去。
//
// 每次启动都这么做一次，好处是 Manifest 不会无限增长 ——
// 否则一个跑了几个月的数据库，启动时要重放几百万条 edit。
func (vs *VersionSet) CreateManifest(logNum uint64) error {
	vs.logNum = logNum

	num := vs.NextFileNum()
	name := fmt.Sprintf("%s%06d", manifestPrefix, num)
	path := filepath.Join(vs.dir, name)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("version: 创建 Manifest 失败: %w", err)
	}

	// 第一条记录是完整快照：把当前所有文件都写进去。
	// 之后的记录都是增量。
	snapshot := &VersionEdit{}
	snapshot.SetLogNumber(vs.logNum)
	snapshot.SetNextFileNum(vs.nextFileNum)
	snapshot.SetLastSequence(vs.lastSeq)
	for level := 0; level < MaxLevels; level++ {
		for _, m := range vs.current.Files(level) {
			snapshot.AddFile(level, *m)
		}
	}

	w := wal.NewWriter(f)
	if err := w.Write(snapshot.Encode()); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("version: 写 Manifest 快照失败: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}

	// 原子切换：先写 CURRENT.tmp，再 rename。
	// 崩溃时 CURRENT 要么是旧内容要么是新内容，绝不会是半行文件名。
	if err := writeCurrentAtomic(vs.dir, name); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}

	oldFile, oldNum := vs.manifestFile, vs.manifestNum
	vs.manifestFile, vs.manifest, vs.manifestNum = f, w, num

	// 旧 Manifest 已经没人需要了 —— CURRENT 已经指向新的，
	// 即使此刻崩溃，重启也只会读新的那个。
	//
	// 注意判断条件是 oldNum 而不是 oldFile：Recover 只读不写，
	// 那条路径上 manifestFile 是 nil，但 manifestNum 是有值的，
	// 旧文件同样需要删除。
	if oldFile != nil {
		oldFile.Close()
	}
	if oldNum != 0 && oldNum != num {
		os.Remove(filepath.Join(vs.dir, fmt.Sprintf("%s%06d", manifestPrefix, oldNum)))
	}
	return nil
}

// writeCurrentAtomic 原子地更新 CURRENT 文件。
func writeCurrentAtomic(dir, manifestName string) error {
	tmp := filepath.Join(dir, currentFile+".tmp")
	if err := os.WriteFile(tmp, []byte(manifestName+"\n"), 0o644); err != nil {
		return fmt.Errorf("version: 写 CURRENT.tmp 失败: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, currentFile)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("version: 切换 CURRENT 失败: %w", err)
	}
	return nil
}

func parseManifestNum(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimPrefix(name, manifestPrefix), 10, 64)
}

// LiveFiles 返回所有【还被任何存活版本引用】的文件编号。
//
// 清理磁盘时用：不在这个集合里的 .sst 文件都可以安全删除。
// compaction 产生的废弃文件就是这样被回收的 ——
// 不是"删完就删"，而是等到没有读者还在用它。
func (vs *VersionSet) LiveFiles() map[uint64]bool {
	live := make(map[uint64]bool)
	for v := range vs.live {
		for level := 0; level < MaxLevels; level++ {
			for _, f := range v.Files(level) {
				live[f.Num] = true
			}
		}
	}
	return live
}

// Close 关闭 Manifest 文件。
func (vs *VersionSet) Close() error {
	if vs.manifestFile == nil {
		return nil
	}
	err := vs.manifestFile.Sync()
	if cerr := vs.manifestFile.Close(); err == nil {
		err = cerr
	}
	vs.manifestFile, vs.manifest = nil, nil
	return err
}
