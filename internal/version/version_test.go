package version

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/leyouhong/shale/internal/ikey"
)

// meta 构造一个 FileMeta，key 范围用用户 key 表示。
func meta(num uint64, smallest, largest string, seq uint64) FileMeta {
	return FileMeta{
		Num:      num,
		Size:     1024,
		Smallest: ikey.Encode(nil, []byte(smallest), seq, ikey.KindSet),
		Largest:  ikey.Encode(nil, []byte(largest), seq, ikey.KindSet),
		MaxSeq:   seq,
	}
}

// ── VersionEdit 编解码 ──────────────────────────────────────

func TestEditRoundTrip(t *testing.T) {
	e := &VersionEdit{}
	e.SetLogNumber(7)
	e.SetNextFileNum(42)
	e.SetLastSequence(12345)
	e.AddFile(0, meta(1, "a", "m", 100))
	e.AddFile(1, meta(2, "n", "z", 200))
	e.DeleteFile(0, 99)

	var got VersionEdit
	if err := got.Decode(e.Encode()); err != nil {
		t.Fatalf("Decode 失败: %v", err)
	}

	if got.LogNumber != 7 || got.NextFileNum != 42 || got.LastSequence != 12345 {
		t.Errorf("全局字段不一致: %+v", got)
	}
	if len(got.NewFiles) != 2 || len(got.DeletedFiles) != 1 {
		t.Fatalf("文件数不一致: +%d -%d", len(got.NewFiles), len(got.DeletedFiles))
	}
	if got.NewFiles[0].Meta.Num != 1 || got.NewFiles[0].Level != 0 {
		t.Errorf("第一个新文件不对: %+v", got.NewFiles[0])
	}
	if got.NewFiles[1].Level != 1 {
		t.Errorf("第二个新文件的层号 = %d，期望 1", got.NewFiles[1].Level)
	}
	if !bytes.Equal(got.NewFiles[0].Meta.Smallest, e.NewFiles[0].Meta.Smallest) {
		t.Error("key 范围没保住")
	}
	if got.DeletedFiles[0].Num != 99 {
		t.Errorf("删除记录不对: %+v", got.DeletedFiles[0])
	}
}

func TestEditEmpty(t *testing.T) {
	var e VersionEdit
	if !e.Empty() {
		t.Error("新建的 edit 应该是空的")
	}
	if len(e.Encode()) != 0 {
		t.Error("空 edit 不应产生任何字节")
	}
	e.AddFile(0, meta(1, "a", "b", 1))
	if e.Empty() {
		t.Error("加了文件之后不该还是空的")
	}
}

func TestEditDecodeCorrupt(t *testing.T) {
	e := &VersionEdit{}
	e.AddFile(0, meta(1, "a", "z", 5))
	data := e.Encode()

	for cut := 1; cut < len(data); cut++ {
		var got VersionEdit
		if err := got.Decode(data[:cut]); err == nil {
			t.Errorf("截断到 %d 字节应该报错", cut)
		}
	}

	// 未知 tag 也要能识别出来
	if err := (&VersionEdit{}).Decode([]byte{99}); err == nil {
		t.Error("未知 tag 应该报错")
	}
}

// ── Version ────────────────────────────────────────────────

func TestVersionApply(t *testing.T) {
	vs := NewVersionSet(t.TempDir())
	v := vs.Current().clone()

	e := &VersionEdit{}
	e.AddFile(0, meta(3, "c", "d", 3))
	e.AddFile(0, meta(1, "a", "b", 1))
	e.AddFile(1, meta(2, "x", "z", 2))
	if err := v.apply(e); err != nil {
		t.Fatal(err)
	}

	if v.NumFiles(0) != 2 || v.NumFiles(1) != 1 {
		t.Fatalf("文件分布不对: L0=%d L1=%d", v.NumFiles(0), v.NumFiles(1))
	}
	// L0 按文件编号排序（编号越大越新）
	if v.Files(0)[0].Num != 1 || v.Files(0)[1].Num != 3 {
		t.Errorf("L0 应按编号排序，实际 %d,%d", v.Files(0)[0].Num, v.Files(0)[1].Num)
	}
	if v.TotalFiles() != 3 {
		t.Errorf("TotalFiles = %d，期望 3", v.TotalFiles())
	}
}

func TestVersionApplyDelete(t *testing.T) {
	vs := NewVersionSet(t.TempDir())
	v := vs.Current().clone()

	add := &VersionEdit{}
	add.AddFile(0, meta(1, "a", "b", 1))
	add.AddFile(0, meta(2, "c", "d", 2))
	v.apply(add)

	del := &VersionEdit{}
	del.DeleteFile(0, 1)
	if err := v.apply(del); err != nil {
		t.Fatal(err)
	}
	if v.NumFiles(0) != 1 || v.Files(0)[0].Num != 2 {
		t.Errorf("删除后应只剩文件 2，实际 %d 个", v.NumFiles(0))
	}

	// 删一个不存在的文件必须报错 —— 说明元数据已经不一致了
	bad := &VersionEdit{}
	bad.DeleteFile(0, 999)
	if err := v.apply(bad); err == nil {
		t.Error("删除不存在的文件应该报错")
	}
}

// TestOverlappingFiles 验证 key 范围重叠判断 ——
// compaction 靠它决定要卷入哪些文件。
func TestOverlappingFiles(t *testing.T) {
	vs := NewVersionSet(t.TempDir())
	v := vs.Current().clone()

	e := &VersionEdit{}
	e.AddFile(1, meta(1, "a", "f", 1))
	e.AddFile(1, meta(2, "g", "m", 2))
	e.AddFile(1, meta(3, "n", "z", 3))
	v.apply(e)

	cases := []struct {
		lo, hi string
		want   []uint64
	}{
		{"a", "c", []uint64{1}},       // 只碰第一个
		{"e", "h", []uint64{1, 2}},    // 跨两个
		{"a", "z", []uint64{1, 2, 3}}, // 全覆盖
		{"g", "g", []uint64{2}},       // 单点落在中间那个
		{"0", "0", nil},               // 比最小的还小
		{"zz", "zzz", nil},            // 比最大的还大
		{"f", "g", []uint64{1, 2}},    // 正好卡在边界上
	}
	for _, c := range cases {
		got := v.OverlappingFiles(1, []byte(c.lo), []byte(c.hi))
		if len(got) != len(c.want) {
			t.Errorf("[%s,%s] 得到 %d 个文件，期望 %d 个", c.lo, c.hi, len(got), len(c.want))
			continue
		}
		for i := range c.want {
			if got[i].Num != c.want[i] {
				t.Errorf("[%s,%s] 第 %d 个是 %d，期望 %d", c.lo, c.hi, i, got[i].Num, c.want[i])
			}
		}
	}
}

// TestRefCounting 验证引用计数 —— compaction 删文件的安全性全靠它。
func TestRefCounting(t *testing.T) {
	vs := NewVersionSet(t.TempDir())

	v1 := vs.Current()
	if v1.Refs() != 1 {
		t.Errorf("VersionSet 应持有 1 个引用，实际 %d", v1.Refs())
	}

	// 模拟一个迭代器持有 v1
	v1.Ref()
	if v1.Refs() != 2 {
		t.Errorf("Refs = %d，期望 2", v1.Refs())
	}

	// 产生新版本：VersionSet 转而指向 v2，但 v1 还有迭代器在用
	e := &VersionEdit{}
	e.AddFile(0, meta(1, "a", "b", 1))
	if err := vs.LogAndApply(e); err != nil {
		t.Fatal(err)
	}

	if v1.Refs() != 1 {
		t.Errorf("v1 应只剩迭代器的引用，实际 %d", v1.Refs())
	}
	if !vs.live[v1] {
		t.Error("v1 还有人用，不该从存活集合里消失")
	}
	// v1 看到的仍是它那一刻的快照
	if v1.TotalFiles() != 0 {
		t.Errorf("v1 是旧快照，不该看到新文件，实际有 %d 个", v1.TotalFiles())
	}
	if vs.Current().TotalFiles() != 1 {
		t.Errorf("新版本应有 1 个文件，实际 %d 个", vs.Current().TotalFiles())
	}

	// 迭代器用完
	v1.Unref()
	if vs.live[v1] {
		t.Error("引用归零后 v1 应从存活集合里移除")
	}
}

func TestUnrefBelowZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("引用计数变负应该 panic —— 这说明 Ref/Unref 不配对")
		}
	}()
	vs := NewVersionSet(t.TempDir())
	v := vs.Current()
	v.Unref() // 这一下归零
	v.Unref() // 这一下变负
}

func TestLiveFiles(t *testing.T) {
	vs := NewVersionSet(t.TempDir())

	e1 := &VersionEdit{}
	e1.AddFile(0, meta(1, "a", "b", 1))
	e1.AddFile(0, meta(2, "c", "d", 2))
	vs.LogAndApply(e1)

	old := vs.Current()
	old.Ref() // 假装有个迭代器在用

	// 删掉文件 1
	e2 := &VersionEdit{}
	e2.DeleteFile(0, 1)
	vs.LogAndApply(e2)

	// 文件 1 虽然从当前版本移除了，但旧版本还在引用它 —— 不能删
	live := vs.LiveFiles()
	if !live[1] {
		t.Error("还有旧版本引用文件 1，它不该被判定为可删除")
	}
	if !live[2] {
		t.Error("文件 2 在当前版本里，必须是存活的")
	}

	old.Unref()
	live = vs.LiveFiles()
	if live[1] {
		t.Error("旧版本释放后，文件 1 就可以删了")
	}
	if !live[2] {
		t.Error("文件 2 仍应存活")
	}
}

// ── Manifest 持久化 ─────────────────────────────────────────

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)
	if err := vs.CreateManifest(1); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		e := &VersionEdit{}
		e.AddFile(0, meta(i*10, fmt.Sprintf("k%d", i), fmt.Sprintf("k%d", i+1), i*100))
		e.SetLastSequence(i * 100)
		if err := vs.LogAndApply(e); err != nil {
			t.Fatal(err)
		}
	}
	wantNext := vs.PeekNextFileNum()
	if err := vs.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开
	vs2 := NewVersionSet(dir)
	ok, err := vs2.Recover()
	if err != nil {
		t.Fatalf("Recover 失败: %v", err)
	}
	if !ok {
		t.Fatal("应该找到已有的 Manifest")
	}

	if vs2.Current().NumFiles(0) != 5 {
		t.Errorf("恢复出 %d 个文件，期望 5 个", vs2.Current().NumFiles(0))
	}
	if vs2.LastSequence() != 500 {
		t.Errorf("LastSequence = %d，期望 500", vs2.LastSequence())
	}
	if vs2.PeekNextFileNum() < wantNext {
		t.Errorf("NextFileNum = %d，不应小于关闭前的 %d", vs2.PeekNextFileNum(), wantNext)
	}
	for i := uint64(1); i <= 5; i++ {
		found := false
		for _, f := range vs2.Current().Files(0) {
			if f.Num == i*10 {
				found = true
				if f.MaxSeq != i*100 {
					t.Errorf("文件 %d 的 MaxSeq = %d，期望 %d", f.Num, f.MaxSeq, i*100)
				}
			}
		}
		if !found {
			t.Errorf("文件 %d 没恢复出来", i*10)
		}
	}
}

// TestManifestRotation 验证每次启动都会换一个新 Manifest，旧的被删掉 ——
// 否则跑久了启动时要重放几百万条 edit。
func TestManifestRotation(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)
	vs.CreateManifest(1)
	e := &VersionEdit{}
	e.AddFile(0, meta(1, "a", "b", 1))
	vs.LogAndApply(e)
	firstManifest := vs.manifestNum
	vs.Close()

	vs2 := NewVersionSet(dir)
	vs2.Recover()
	if err := vs2.CreateManifest(2); err != nil {
		t.Fatal(err)
	}
	defer vs2.Close()

	if vs2.manifestNum == firstManifest {
		t.Error("启动后应该换一个新的 Manifest")
	}
	// 旧的应该被删掉了
	oldPath := filepath.Join(dir, fmt.Sprintf("%s%06d", manifestPrefix, firstManifest))
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Error("旧 Manifest 应该被删除")
	}
	// 数据没丢
	if vs2.Current().NumFiles(0) != 1 {
		t.Errorf("换 Manifest 后文件数 = %d，期望 1", vs2.Current().NumFiles(0))
	}
}

func TestRecoverFreshDatabase(t *testing.T) {
	vs := NewVersionSet(t.TempDir())
	ok, err := vs.Recover()
	if err != nil {
		t.Fatalf("全新数据库 Recover 不该报错: %v", err)
	}
	if ok {
		t.Error("全新数据库不该找到 Manifest")
	}
	if vs.Current().TotalFiles() != 0 {
		t.Error("全新数据库不该有文件")
	}
}

func TestCurrentFileIsAtomic(t *testing.T) {
	dir := t.TempDir()
	vs := NewVersionSet(dir)
	if err := vs.CreateManifest(1); err != nil {
		t.Fatal(err)
	}
	defer vs.Close()

	// CURRENT 里必须是一个存在的 Manifest 文件名
	data, err := os.ReadFile(filepath.Join(dir, currentFile))
	if err != nil {
		t.Fatal(err)
	}
	name := string(bytes.TrimSpace(data))
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		t.Errorf("CURRENT 指向的 %q 不存在: %v", name, err)
	}
	// 不该留下临时文件
	if _, err := os.Stat(filepath.Join(dir, currentFile+".tmp")); !os.IsNotExist(err) {
		t.Error("CURRENT.tmp 应该已经被 rename 掉了")
	}
}

func TestManifestSurvivesTruncation(t *testing.T) {
	dir := t.TempDir()

	vs := NewVersionSet(dir)
	vs.CreateManifest(1)
	for i := uint64(1); i <= 10; i++ {
		e := &VersionEdit{}
		e.AddFile(0, meta(i, "a", "z", i))
		vs.LogAndApply(e)
	}
	manifestName := fmt.Sprintf("%s%06d", manifestPrefix, vs.manifestNum)
	vs.Close()

	// 模拟崩溃：砍掉 Manifest 的尾巴
	path := filepath.Join(dir, manifestName)
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-10)

	vs2 := NewVersionSet(dir)
	ok, err := vs2.Recover()
	if err != nil {
		t.Fatalf("Manifest 尾部损坏时应该能恢复出前面的部分: %v", err)
	}
	if !ok {
		t.Fatal("应该找到 Manifest")
	}
	n := vs2.Current().NumFiles(0)
	if n == 0 {
		t.Error("应该恢复出一部分文件")
	}
	if n > 10 {
		t.Errorf("恢复出 %d 个文件，超过了原有的 10 个", n)
	}
	t.Logf("Manifest 被截断后恢复出 %d/10 个文件", n)
}
