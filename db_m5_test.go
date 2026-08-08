package shale

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// 这个文件是 M5 的验收测试：范围扫描要有序、去重、跳过墓碑。

// scanAll 遍历整个数据库，返回 "key=value" 列表。
func scanAll(t *testing.T, db *DB) []string {
	t.Helper()
	it, err := db.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator 失败: %v", err)
	}
	defer it.Close()

	var out []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		out = append(out, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("遍历出错: %v", err)
	}
	return out
}

func assertScan(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("扫出 %d 条，期望 %d 条\n实际: %v\n期望: %v",
			len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第 %d 条 = %q，期望 %q", i, got[i], want[i])
		}
	}
}

func TestIteratorEmpty(t *testing.T) {
	db := mustOpen(t)
	it, err := db.NewIterator()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	it.SeekToFirst()
	if it.Valid() {
		t.Error("空数据库遍历不该有效")
	}
	it.Seek([]byte("anything"))
	if it.Valid() {
		t.Error("空数据库 Seek 不该有效")
	}
}

func TestIteratorBasic(t *testing.T) {
	db := mustOpen(t)
	// 乱序写入
	db.Put([]byte("c"), []byte("3"))
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))

	assertScan(t, scanAll(t, db), []string{"a=1", "b=2", "c=3"})
}

// TestIteratorDedup 验证同一个 key 只输出一次，且是最新版本。
func TestIteratorDedup(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))
	db.Put([]byte("k"), []byte("v3"))
	db.Put([]byte("other"), []byte("x"))

	assertScan(t, scanAll(t, db), []string{"k=v3", "other=x"})
}

// TestIteratorSkipsTombstones 验证被删除的 key 不出现在遍历结果里。
func TestIteratorSkipsTombstones(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("c"), []byte("3"))
	db.Delete([]byte("b"))

	assertScan(t, scanAll(t, db), []string{"a=1", "c=3"})

	// 删了再写回来，应该重新出现
	db.Put([]byte("b"), []byte("back"))
	assertScan(t, scanAll(t, db), []string{"a=1", "b=back", "c=3"})
}

// TestIteratorTombstoneHidesOlderVersions 验证墓碑能遮住同一个 key 的旧版本 ——
// 这是最容易写错的地方：跳过墓碑之后，绝不能接着输出它下面的旧数据。
func TestIteratorTombstoneHidesOlderVersions(t *testing.T) {
	db := mustOpen(t)

	db.Put([]byte("k"), []byte("v1"))
	db.Flush() // v1 进了一个 SSTable
	db.Put([]byte("k"), []byte("v2"))
	db.Flush() // v2 进了另一个
	db.Delete([]byte("k"))
	db.Flush() // 墓碑进了第三个

	db.Put([]byte("z"), []byte("visible"))

	// k 的三个版本分散在三个文件里，墓碑最新 —— 一条都不该出现
	assertScan(t, scanAll(t, db), []string{"z=visible"})
}

// TestIteratorAcrossMemTableAndSSTables 验证跨内存和多个文件的归并。
func TestIteratorAcrossMemTableAndSSTables(t *testing.T) {
	db := mustOpen(t)

	// 故意让数据交错分布在不同文件里
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("d"), []byte("4"))
	db.Flush()

	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("e"), []byte("5"))
	db.Flush()

	db.Put([]byte("c"), []byte("3")) // 这条还在 MemTable 里

	assertScan(t, scanAll(t, db), []string{"a=1", "b=2", "c=3", "d=4", "e=5"})
}

// TestIteratorNewerFileWins 验证跨文件的版本覆盖。
func TestIteratorNewerFileWins(t *testing.T) {
	db := mustOpen(t)

	db.Put([]byte("k"), []byte("old"))
	db.Flush()
	db.Put([]byte("k"), []byte("mid"))
	db.Flush()
	db.Put([]byte("k"), []byte("new")) // 在 MemTable 里，最新

	assertScan(t, scanAll(t, db), []string{"k=new"})
}

func TestIteratorSeek(t *testing.T) {
	db := mustOpen(t)
	for _, k := range []string{"b", "d", "f", "h"} {
		db.Put([]byte(k), []byte("v"+k))
	}
	db.Flush()
	db.Put([]byte("j"), []byte("vj")) // 一条在内存里

	it, _ := db.NewIterator()
	defer it.Close()

	cases := []struct{ seek, want string }{
		{"a", "b"}, {"b", "b"}, {"c", "d"}, {"g", "h"},
		{"i", "j"}, {"j", "j"}, {"k", ""},
	}
	for _, c := range cases {
		it.Seek([]byte(c.seek))
		if c.want == "" {
			if it.Valid() {
				t.Errorf("Seek(%q) 应无效，却停在 %q", c.seek, it.Key())
			}
			continue
		}
		if !it.Valid() {
			t.Errorf("Seek(%q) 应停在 %q，却无效", c.seek, c.want)
			continue
		}
		if got := string(it.Key()); got != c.want {
			t.Errorf("Seek(%q) 停在 %q，期望 %q", c.seek, got, c.want)
		}
	}
}

// TestIteratorPrefixScan 是最常见的实际用法：按前缀扫描。
func TestIteratorPrefixScan(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("user:001"), []byte("alice"))
	db.Put([]byte("user:002"), []byte("bob"))
	db.Put([]byte("user:003"), []byte("carol"))
	db.Put([]byte("post:001"), []byte("hello"))
	db.Put([]byte("zzz"), []byte("last"))
	db.Flush()
	db.Put([]byte("user:004"), []byte("dave"))

	it, _ := db.NewIterator()
	defer it.Close()

	var got []string
	prefix := []byte("user:")
	for it.Seek(prefix); it.Valid(); it.Next() {
		if len(it.Key()) < len(prefix) || string(it.Key()[:len(prefix)]) != "user:" {
			break // 越过前缀范围就停
		}
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	assertScan(t, got, []string{
		"user:001=alice", "user:002=bob", "user:003=carol", "user:004=dave",
	})
}

// TestIteratorSnapshotIsolation 验证迭代器看到的是创建那一刻的快照。
func TestIteratorSnapshotIsolation(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))

	it, _ := db.NewIterator()
	defer it.Close()
	it.SeekToFirst() // 快照在此刻确定

	// 创建之后再写入，迭代器不该看到
	db.Put([]byte("c"), []byte("3"))
	db.Put([]byte("a"), []byte("changed"))
	db.Delete([]byte("b"))

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	assertScan(t, got, []string{"a=1", "b=2"})

	// 新开的迭代器则看到最新状态
	assertScan(t, scanAll(t, db), []string{"a=changed", "c=3"})
}

// TestIteratorHoldsFilesAlive 验证迭代器持有的 Version 引用能保住文件。
func TestIteratorHoldsFilesAlive(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("k"), []byte("v"))
	db.Flush()

	it, _ := db.NewIterator()
	it.SeekToFirst()

	filesHeld := db.vs.Current().Files(0)
	if len(filesHeld) != 1 {
		t.Fatalf("应有 1 个文件，实际 %d 个", len(filesHeld))
	}
	num := filesHeld[0].Num

	// 再刷几次产生新版本
	db.Put([]byte("k2"), []byte("v2"))
	db.Flush()

	// 迭代器还活着，它引用的文件必须仍被判定为存活
	if !db.vs.LiveFiles()[num] {
		t.Error("迭代器持有的文件不该被判定为可删除")
	}

	it.Close()
}

func TestIteratorAfterClose(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("k"), []byte("v"))

	it, _ := db.NewIterator()
	it.SeekToFirst()
	if !it.Valid() {
		t.Fatal("关闭前应该有效")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if it.Valid() {
		t.Error("关闭后不该有效")
	}
	if err := it.Close(); err != nil {
		t.Errorf("重复 Close 应该安全，得到 %v", err)
	}
}

func TestIteratorOnClosedDB(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, nil)
	db.Close()
	if _, err := db.NewIterator(); err != ErrClosed {
		t.Errorf("已关闭的 DB 应返回 ErrClosed，得到 %v", err)
	}
}

// TestIteratorAgainstSortedMap 是 M5 的验收测试：
// 随机构造数据（含大量覆盖、删除、多次 flush），
// 遍历结果必须与「map + 排序」完全一致。
func TestIteratorAgainstSortedMap(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{MemTableSize: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260811))

	const ops = 20000
	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("key%04d", rng.Intn(800))
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5: // 60% 写
			val := fmt.Sprintf("v%d", i)
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			golden[key] = val
		case 6, 7: // 20% 删
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(golden, key)
		case 8: // 10% flush
			if rng.Intn(20) == 0 {
				db.Flush()
			}
		default: // 剩下的做一次全量比对
			if rng.Intn(2000) == 0 {
				checkScanMatches(t, db, golden, i)
			}
		}
	}

	checkScanMatches(t, db, golden, ops)

	// 重启之后遍历结果也必须一致
	db.Close()
	db2, _ := Open(dir, nil)
	defer db2.Close()
	checkScanMatches(t, db2, golden, -1)

	t.Logf("%d 次操作后：%d 个存活 key，%d 个 SSTable",
		ops, len(golden), numTables(db2))
}

// checkScanMatches 把整库扫描的结果与 map 比对。
func checkScanMatches(t *testing.T, db *DB, golden map[string]string, step int) {
	t.Helper()

	want := make([]string, 0, len(golden))
	for k, v := range golden {
		want = append(want, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(want)

	got := scanAll(t, db)
	if len(got) != len(want) {
		t.Fatalf("第 %d 步：扫出 %d 条，期望 %d 条", step, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 步：扫描结果第 %d 条 = %q，期望 %q", step, i, got[i], want[i])
		}
	}
}

// TestIteratorRangeScanMatchesGet 验证扫描和点查的结果一致 ——
// 两条读路径的过滤逻辑是分别实现的，必须互相印证。
func TestIteratorRangeScanMatchesGet(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{MemTableSize: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(77))
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("k%04d", rng.Intn(300))
		if rng.Intn(4) == 0 {
			db.Delete([]byte(key))
		} else {
			db.Put([]byte(key), []byte(fmt.Sprintf("v%d", i)))
		}
	}

	it, _ := db.NewIterator()
	defer it.Close()

	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		scanVal := string(it.Value())

		getVal, err := db.Get(key)
		if err != nil {
			t.Fatalf("扫描扫到了 %q，但 Get 说它不存在: %v", key, err)
		}
		if string(getVal) != scanVal {
			t.Fatalf("%q：扫描得到 %q，Get 得到 %q", key, scanVal, getVal)
		}
		n++
	}
	t.Logf("扫描出 %d 个 key，每个都与 Get 的结果一致", n)
}

func BenchmarkIteratorScan(b *testing.B) {
	db, err := Open(b.TempDir(), &Options{MemTableSize: 64 << 10})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 50000
	val := make([]byte, 100)
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%06d", i)), val)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, err := db.NewIterator()
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for it.SeekToFirst(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("扫出 %d 条，期望 %d 条", count, n)
		}
	}
}
