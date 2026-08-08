package version

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/leyouhong/shale/internal/ikey"
)

// MaxLevels 是最大层数。
const MaxLevels = 7

// Version 是某一时刻【完整的文件集合】，一旦创建就不再修改。
//
// # 为什么要做成不可变的 + 引用计数
//
// compaction 会在后台删文件，而前台可能正有一个迭代器在读它。
// 如果直接改一个共享的"当前文件列表"，读到一半文件没了就会出错。
//
// 做法是：每次变更【生成一个新 Version】，旧的留着不动。
// 谁在用就 Ref 一下，用完 Unref；
// 只有当一个 Version 的引用归零、且它引用的文件不再属于任何存活 Version 时，
// 那些文件才真正从磁盘上删除。
//
//	读者持有 v1 ──┐
//	              ├─→ 都指向自己那一刻的文件集合，互不干扰
//	compaction    │
//	产生了 v2  ───┘
//
// 这套机制顺带就实现了「迭代器看到的是创建那一刻的快照」——
// 不需要额外做 MVCC。
type Version struct {
	// files[level] 是该层的文件列表。
	//
	// L0 按文件编号从小到大排（编号越大越新，查找要从后往前）；
	// L1 及以下按 key 范围从小到大排且互不重叠，可以二分定位。
	files [][]*FileMeta

	refs int
	vs   *VersionSet
}

func newVersion(vs *VersionSet) *Version {
	return &Version{
		files: make([][]*FileMeta, MaxLevels),
		vs:    vs,
	}
}

// Files 返回某一层的文件列表。调用方不要修改返回的切片。
func (v *Version) Files(level int) []*FileMeta {
	if level < 0 || level >= len(v.files) {
		return nil
	}
	return v.files[level]
}

// NumFiles 返回某一层的文件数。
func (v *Version) NumFiles(level int) int { return len(v.Files(level)) }

// TotalFiles 返回所有层的文件总数。
func (v *Version) TotalFiles() int {
	n := 0
	for _, f := range v.files {
		n += len(f)
	}
	return n
}

// LevelSize 返回某一层的总字节数。
func (v *Version) LevelSize(level int) int64 {
	var n int64
	for _, f := range v.Files(level) {
		n += f.Size
	}
	return n
}

// Ref 增加一次引用。持有 Version 期间它引用的文件保证不会被删除。
func (v *Version) Ref() { v.refs++ }

// Unref 减少一次引用。归零时触发废弃文件的回收。
func (v *Version) Unref() {
	v.refs--
	if v.refs < 0 {
		panic("version: 引用计数变成负数，说明 Ref/Unref 不配对")
	}
	if v.refs == 0 && v.vs != nil {
		v.vs.removeVersion(v)
	}
}

// Refs 返回当前引用数，仅用于测试和调试。
func (v *Version) Refs() int { return v.refs }

// OverlappingFiles 返回某一层里 key 范围与 [smallest, largest] 有交集的文件。
//
// 参数 smallest 和 largest 是【用户 key】，不是内部 key ——
// 判断重叠必须按用户 key 来：同一个用户 key 的所有版本
// 必须被同一次 compaction 一起处理，否则新旧版本会被拆散到不同文件里。
//
// （FileMeta.Smallest/Largest 存的是内部 key，所以下面要先剥掉 trailer。
// 这两种 key 混用是很容易出错的地方，凡是涉及边界比较都要想清楚用哪个。）
//
// compaction 靠它决定"要卷入哪些文件"：
// 从上层挑一个文件后，下层所有与之重叠的都必须一起参与归并，
// 否则合并出来的结果会和下层已有数据交错，破坏"层内不重叠"的性质。
func (v *Version) OverlappingFiles(level int, smallest, largest []byte) []*FileMeta {
	var out []*FileMeta
	for _, f := range v.Files(level) {
		// 不重叠的两种情况：文件整个在左边，或者整个在右边
		if bytes.Compare(ikey.UserKey(f.Largest), smallest) < 0 {
			continue
		}
		if bytes.Compare(ikey.UserKey(f.Smallest), largest) > 0 {
			continue
		}
		out = append(out, f)
	}
	return out
}

// String 输出各层的文件分布，便于观察。
func (v *Version) String() string {
	var b strings.Builder
	for level := 0; level < len(v.files); level++ {
		if len(v.files[level]) == 0 {
			continue
		}
		fmt.Fprintf(&b, "L%d: %d 个文件", level, len(v.files[level]))
		for _, f := range v.files[level] {
			fmt.Fprintf(&b, " [%06d %s~%s]", f.Num,
				shortKey(f.Smallest), shortKey(f.Largest))
		}
		b.WriteByte('\n')
	}
	if b.Len() == 0 {
		return "(空)\n"
	}
	return b.String()
}

func shortKey(ik []byte) string {
	if len(ik) < ikey.TrailerSize {
		return "?"
	}
	uk := ikey.UserKey(ik)
	if len(uk) > 8 {
		return string(uk[:8]) + ".."
	}
	return string(uk)
}

// clone 复制出一个新 Version（文件列表浅拷贝，FileMeta 本身共享）。
//
// FileMeta 创建之后就不再改，所以多个 Version 共享同一个 FileMeta 是安全的。
func (v *Version) clone() *Version {
	nv := newVersion(v.vs)
	for level := range v.files {
		nv.files[level] = append([]*FileMeta(nil), v.files[level]...)
	}
	return nv
}

// apply 把一条 edit 应用到自身，得到变更后的文件集合。
// 只在构造新 Version 时调用。
func (v *Version) apply(e *VersionEdit) error {
	// 先删后加：同一条 edit 里可能既删旧文件又加新文件（compaction 就是这样）
	for _, d := range e.DeletedFiles {
		if d.Level < 0 || d.Level >= len(v.files) {
			return fmt.Errorf("%w: 层号 %d 越界", ErrCorrupt, d.Level)
		}
		files := v.files[d.Level]
		idx := -1
		for i, f := range files {
			if f.Num == d.Num {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("%w: 要删除的文件 %06d 不在 L%d 里", ErrCorrupt, d.Num, d.Level)
		}
		v.files[d.Level] = append(files[:idx:idx], files[idx+1:]...)
	}

	for _, n := range e.NewFiles {
		if n.Level < 0 || n.Level >= len(v.files) {
			return fmt.Errorf("%w: 层号 %d 越界", ErrCorrupt, n.Level)
		}
		m := n.Meta
		v.files[n.Level] = append(v.files[n.Level], &m)
	}

	v.sortLevels()
	return nil
}

// sortLevels 让每层的文件保持约定的顺序。
func (v *Version) sortLevels() {
	// L0 的文件 key 范围互相重叠，只能按【新旧】排序：编号越大越新。
	sort.Slice(v.files[0], func(i, j int) bool {
		return v.files[0][i].Num < v.files[0][j].Num
	})
	// L1 及以下层内不重叠，按 key 范围排序，将来可以二分定位。
	for level := 1; level < len(v.files); level++ {
		files := v.files[level]
		sort.Slice(files, func(i, j int) bool {
			return ikey.Compare(files[i].Smallest, files[j].Smallest) < 0
		})
	}
}
