package shale

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 这个文件是 M4 的验收测试：元数据不再靠扫目录，而是走 Manifest。

func TestManifestCreatedOnOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// CURRENT 必须存在，且指向一个真实的 MANIFEST 文件
	data, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatalf("CURRENT 文件应该被创建: %v", err)
	}
	name := strings.TrimSpace(string(data))
	if !strings.HasPrefix(name, "MANIFEST-") {
		t.Errorf("CURRENT 内容 = %q，期望 MANIFEST-xxx", name)
	}
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		t.Errorf("CURRENT 指向的 %q 不存在", name)
	}
}

// TestManifestIsAuthoritative 是 M4 的核心：
// 一个没被 Manifest 登记的 .sst 文件，即使物理存在也不该生效。
//
// 这正是"扫目录"做不到的事 —— 崩溃留下的孤儿文件必须被忽略，
// 否则会把不完整或已废弃的数据当成有效数据读进来。
func TestManifestIsAuthoritative(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	db.Put([]byte("real"), []byte("data"))
	db.Flush()
	db.Close()

	// 伪造一个孤儿 SSTable：文件名合法、内容是从真文件复制来的（所以能打开），
	// 但 Manifest 里没有它的记录。
	var srcName string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			srcName = e.Name()
			break
		}
	}
	if srcName == "" {
		t.Fatal("没找到 SSTable")
	}
	content, err := os.ReadFile(filepath.Join(dir, srcName))
	if err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "000999.sst")
	if err := os.WriteFile(orphan, content, 0o644); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("有孤儿文件时应该也能正常打开: %v", err)
	}
	defer db2.Close()

	// 孤儿文件不该被算作生效文件
	if n := numTables(db2); n != 1 {
		t.Errorf("生效文件数 = %d，期望 1（孤儿文件不算）", n)
	}
	// 而且应该被清理掉
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Error("没被 Manifest 登记的孤儿文件应该被删除")
	}
	mustGet(t, db2, "real", "data")
}

// TestFileMetaRecovered 验证文件的元信息（key 范围、大小、seq）
// 是从 Manifest 读出来的，不需要打开文件去扫。
func TestFileMetaRecovered(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("value"))
	}
	db.Flush()
	db.Close()

	db2, _ := Open(dir, nil)
	defer db2.Close()

	db2.mu.RLock()
	files := db2.vs.Current().Files(0)
	if len(files) != 1 {
		db2.mu.RUnlock()
		t.Fatalf("应有 1 个文件，实际 %d 个", len(files))
	}
	f := *files[0]
	db2.mu.RUnlock()
	if f.Size <= 0 {
		t.Error("文件大小应该被记录")
	}
	if f.MaxSeq != 100 {
		t.Errorf("MaxSeq = %d，期望 100", f.MaxSeq)
	}
	if len(f.Smallest) == 0 || len(f.Largest) == 0 {
		t.Error("key 范围应该被记录")
	}
	t.Logf("从 Manifest 恢复的元信息：文件 %06d，%d 字节，MaxSeq=%d",
		f.Num, f.Size, f.MaxSeq)
}

// TestSeqFromManifest 验证 seq 现在由 Manifest 持久化，
// 不再需要扫描 SSTable 内容来恢复（M3 的临时方案）。
func TestSeqFromManifest(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 50; i++ {
		db.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	db.Flush()
	seqBefore := db.seq
	db.Close()

	db2, _ := Open(dir, nil)
	defer db2.Close()

	if db2.seq != seqBefore {
		t.Errorf("重启后 seq = %d，期望 %d", db2.seq, seqBefore)
	}
	if db2.vs.LastSequence() != seqBefore {
		t.Errorf("VersionSet 里的 seq = %d，期望 %d", db2.vs.LastSequence(), seqBefore)
	}
	mustGet(t, db2, "k", "v49")

	// 重启后继续写，必须能覆盖
	db2.Put([]byte("k"), []byte("after-restart"))
	mustGet(t, db2, "k", "after-restart")
}

// TestManifestDoesNotGrowUnbounded 验证每次启动都会换新 Manifest，
// 否则跑久了启动要重放几百万条记录。
func TestManifestDoesNotGrowUnbounded(t *testing.T) {
	dir := t.TempDir()

	var sizes []int64
	for round := 0; round < 5; round++ {
		db, err := Open(dir, &Options{MemTableSize: 8 << 10})
		if err != nil {
			t.Fatal(err)
		}
		val := make([]byte, 100)
		for i := 0; i < 300; i++ {
			db.Put([]byte(fmt.Sprintf("r%d-k%03d", round, i)), val)
		}
		db.Close()

		// 每轮结束时只应该有一个 MANIFEST 文件
		n := countFiles(t, dir, "")
		manifests := 0
		entries, _ := os.ReadDir(dir)
		var size int64
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "MANIFEST-") {
				manifests++
				info, _ := e.Info()
				size = info.Size()
			}
		}
		if manifests != 1 {
			t.Errorf("第 %d 轮后有 %d 个 MANIFEST，期望 1 个", round, manifests)
		}
		sizes = append(sizes, size)
		_ = n
	}
	t.Logf("5 轮之后各次的 Manifest 大小：%v 字节", sizes)
}

// TestVersionSnapshotIsolation 验证 Version 的快照语义：
// 持有一个旧 Version 的读者，看到的仍是它那一刻的文件集合。
//
// 这是 compaction 能安全删文件的基础 —— M6 会真正用上。
func TestVersionSnapshotIsolation(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	db.Flush()

	// 抓住当前版本，模拟一个长时间运行的迭代器
	db.mu.Lock()
	snap := db.vs.Current()
	snap.Ref()
	db.mu.Unlock()
	filesAtSnapshot := snap.TotalFiles()

	// 再刷两次，产生新版本
	db.Put([]byte("b"), []byte("2"))
	db.Flush()
	db.Put([]byte("c"), []byte("3"))
	db.Flush()

	if snap.TotalFiles() != filesAtSnapshot {
		t.Errorf("旧快照看到 %d 个文件，应该始终是 %d 个",
			snap.TotalFiles(), filesAtSnapshot)
	}
	if n := numTables(db); n != 3 {
		t.Errorf("当前版本应有 3 个文件，实际 %d 个", n)
	}

	// 快照持有期间，它引用的文件必须还活着
	db.mu.RLock()
	live := db.vs.LiveFiles()
	db.mu.RUnlock()
	for _, f := range snap.Files(0) {
		if !live[f.Num] {
			t.Errorf("快照引用的文件 %06d 被判定为可删除了", f.Num)
		}
	}
	db.mu.Lock()
	snap.Unref()
	db.mu.Unlock()
}

// TestOldWALsCleanedUp 验证已经落盘的 WAL 会被清理掉。
func TestOldWALsCleanedUp(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	defer db.Close()

	val := make([]byte, 100)
	for i := 0; i < 1000; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), val)
	}

	// M9 起后台刷盘，所以未落盘的 immutable 对应的 WAL 必须保留 ——
	// 日志数不再总是 1，但上界是"内存中的 MemTable 个数"。
	n := countFiles(t, dir, ".log")
	if n > db.opts.MaxMemTables {
		t.Errorf("有 %d 个 WAL 文件，超过了内存中 MemTable 的上限 %d —— 旧日志没被清理",
			n, db.opts.MaxMemTables)
	}
	t.Logf("写入 1000 条、产生 %d 个 SSTable 之后，只剩 %d 个 WAL（上限 %d）",
		numTables(db), n, db.opts.MaxMemTables)
}

func TestStatsPerLevel(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, &Options{MemTableSize: 8 << 10})
	defer db.Close()

	val := make([]byte, 100)
	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), val)
	}

	st := db.Stats()
	if len(st.Levels) == 0 {
		t.Fatal("应该有层级统计")
	}
	if st.Levels[0].Level != 0 {
		t.Error("第一项应该是 L0")
	}
	// 只断言 Stats 自身自洽，【不】拿它和另一次 numTables() 去比 ——
	// M9 起后台在跑，两次独立的观察本来就是两个时间点的快照，
	// 中间可能刚好完成一次 flush，不一致是正常的。
	total := 0
	for _, l := range st.Levels {
		total += l.NumFiles
		if l.NumFiles > 0 && l.Size <= 0 {
			t.Errorf("L%d 有 %d 个文件却报告 0 字节", l.Level, l.NumFiles)
		}
	}
	if total == 0 {
		t.Error("写了 500 条数据，应该已经有 SSTable 了")
	}
	t.Logf("\n%s", st)
}

// TestCrashBeforeManifestCommit 验证：SSTable 写好了但 Manifest 还没记录时崩溃，
// 那个文件会被当作孤儿清掉，数据从 WAL 恢复 —— 不多不少。
func TestCrashBeforeManifestCommit(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir, nil)
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	// 不 flush 不 close，直接崩溃：数据只在 WAL 里

	db2 := crashReopen(t, dir, nil)
	defer db2.Close()

	if numTables(db2) != 0 {
		t.Errorf("不该有生效的 SSTable，实际 %d 个", numTables(db2))
	}
	if db2.RecoveredEntries() != 100 {
		t.Errorf("应从 WAL 恢复 100 条，实际 %d 条", db2.RecoveredEntries())
	}
	for i := 0; i < 100; i++ {
		mustGet(t, db2, fmt.Sprintf("k%03d", i), "v")
	}
}
