package shale

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

// 这个文件是 M1 的验收测试：读写路径接通后的功能验证。

func mustOpen(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// mustGet 断言某个 key 存在且值符合预期。
func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) 出错: %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q，期望 %q", key, got, want)
	}
}

// mustMiss 断言某个 key 不存在。
func mustMiss(t *testing.T, db *DB, key string) {
	t.Helper()
	_, err := db.Get([]byte(key))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) 应返回 ErrNotFound，得到 %v", key, err)
	}
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t)

	mustMiss(t, db, "k")

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put 失败: %v", err)
	}
	mustGet(t, db, "k", "v")

	// 覆盖
	if err := db.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("覆盖失败: %v", err)
	}
	mustGet(t, db, "k", "v2")

	// 删除
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}
	mustMiss(t, db, "k")

	// 删了还能写回来
	if err := db.Put([]byte("k"), []byte("v3")); err != nil {
		t.Fatalf("重新写入失败: %v", err)
	}
	mustGet(t, db, "k", "v3")
}

func TestDeleteNonexistentIsOK(t *testing.T) {
	db := mustOpen(t)
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Errorf("删除不存在的 key 不应报错，得到 %v", err)
	}
	mustMiss(t, db, "never-existed")
}

// TestEmptyValueVsDeletion 验证「空值」和「已删除」是两回事。
func TestEmptyValueVsDeletion(t *testing.T) {
	db := mustOpen(t)

	if err := db.Put([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Put 空值失败: %v", err)
	}
	got, err := db.Get([]byte("empty"))
	if err != nil {
		t.Fatalf("空值的 key 应该存在，得到 %v", err)
	}
	if len(got) != 0 {
		t.Errorf("值应为空，得到 %q", got)
	}

	if err := db.Put([]byte("nilval"), nil); err != nil {
		t.Fatalf("Put nil 值失败: %v", err)
	}
	if _, err := db.Get([]byte("nilval")); err != nil {
		t.Errorf("写 nil 值的 key 也应该存在，得到 %v", err)
	}
}

// TestGetReturnsCopy 验证返回的切片是拷贝 ——
// 调用方改它不能影响数据库里的数据。
func TestGetReturnsCopy(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("k"), []byte("original"))

	got, _ := db.Get([]byte("k"))
	copy(got, "XXXXXXXX")

	mustGet(t, db, "k", "original")
}

// TestPutCopiesInput 验证 Put 之后修改调用方的缓冲区不影响已写入的数据。
func TestPutCopiesInput(t *testing.T) {
	db := mustOpen(t)
	key := []byte("mykey")
	val := []byte("myval")
	db.Put(key, val)

	copy(key, "XXXXX")
	copy(val, "YYYYY")

	mustGet(t, db, "mykey", "myval")
}

func TestBatchAtomicity(t *testing.T) {
	db := mustOpen(t)
	db.Put([]byte("existing"), []byte("old"))

	b := NewBatch()
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("existing"))
	b.Put([]byte("c"), []byte("3"))

	if err := db.Write(b); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}

	mustGet(t, db, "a", "1")
	mustGet(t, db, "b", "2")
	mustGet(t, db, "c", "3")
	mustMiss(t, db, "existing")
}

// TestBatchInternalOrder 验证同一批里对同一个 key 的多次操作按顺序生效。
// 这依赖「第 i 条记录拿到 base+i」这个序号分配规则。
func TestBatchInternalOrder(t *testing.T) {
	db := mustOpen(t)

	b := NewBatch()
	b.Put([]byte("k"), []byte("first"))
	b.Delete([]byte("k"))
	b.Put([]byte("k"), []byte("last"))
	if err := db.Write(b); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	mustGet(t, db, "k", "last")

	b2 := NewBatch()
	b2.Put([]byte("k2"), []byte("value"))
	b2.Delete([]byte("k2")) // 最后一步是删除
	if err := db.Write(b2); err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	mustMiss(t, db, "k2")
}

// TestSeqIncreases 验证序号确实在递增 —— 它是 LSM 判断版本新旧的唯一依据。
func TestSeqIncreases(t *testing.T) {
	db := mustOpen(t)

	if db.seq != 0 {
		t.Errorf("初始 seq = %d，期望 0", db.seq)
	}

	db.Put([]byte("a"), []byte("1"))
	if db.seq != 1 {
		t.Errorf("写入 1 条后 seq = %d，期望 1", db.seq)
	}

	b := NewBatch()
	b.Put([]byte("b"), []byte("2"))
	b.Put([]byte("c"), []byte("3"))
	b.Delete([]byte("d"))
	db.Write(b)
	if db.seq != 4 {
		t.Errorf("再写入 3 条后 seq = %d，期望 4", db.seq)
	}

	// 空 batch 不应消耗序号
	db.Write(NewBatch())
	if db.seq != 4 {
		t.Errorf("空 batch 后 seq = %d，不应改变", db.seq)
	}
}

func TestKeysWithArbitraryBytes(t *testing.T) {
	db := mustOpen(t)
	keys := []string{
		"",           // 空 key 合法
		"\x00",       // 零字节
		"\xff\xfe",   // 高位字节
		"with space", //
		"with\nnewline",
		string(bytes.Repeat([]byte("x"), MaxKeySize)), // 正好到上限
	}
	for i, k := range keys {
		v := fmt.Sprintf("value%d", i)
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%q) 失败: %v", k, err)
		}
	}
	for i, k := range keys {
		mustGet(t, db, k, fmt.Sprintf("value%d", i))
	}
}

// TestAgainstGoMap 是 M1 的验收测试，也是整个项目的【安全网】。
//
// 思路：用 Go 原生 map 当"标准答案"，把同一串随机操作同时打给两边，
// 每一步都比对结果。任何不一致立刻暴露。
//
// 这个测试会在后续每个里程碑复用 —— 加了 WAL 跑一遍、加了 SSTable 跑一遍、
// 加了 compaction 再跑一遍。它抓过的 bug 会比所有单元测试加起来还多。
func TestAgainstGoMap(t *testing.T) {
	const (
		ops     = 100000
		keySpan = 2000 // key 空间刻意开小，制造大量覆盖和删除
	)

	db := mustOpen(t)
	golden := make(map[string]string)
	rng := rand.New(rand.NewSource(20260808))

	var puts, deletes, gets, hits int

	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("key%05d", rng.Intn(keySpan))

		switch rng.Intn(10) {
		case 0, 1, 2, 3: // 40% 写入
			val := fmt.Sprintf("val%d-%d", i, rng.Int())
			if err := db.Put([]byte(key), []byte(val)); err != nil {
				t.Fatalf("第 %d 步 Put(%q) 失败: %v", i, key, err)
			}
			golden[key] = val
			puts++

		case 4, 5: // 20% 删除
			if err := db.Delete([]byte(key)); err != nil {
				t.Fatalf("第 %d 步 Delete(%q) 失败: %v", i, key, err)
			}
			delete(golden, key)
			deletes++

		case 6: // 10% 批量写
			b := NewBatch()
			n := 1 + rng.Intn(5)
			for j := 0; j < n; j++ {
				k := fmt.Sprintf("key%05d", rng.Intn(keySpan))
				if rng.Intn(4) == 0 {
					b.Delete([]byte(k))
					delete(golden, k)
				} else {
					v := fmt.Sprintf("batch%d-%d", i, j)
					b.Put([]byte(k), []byte(v))
					golden[k] = v
				}
			}
			if err := db.Write(b); err != nil {
				t.Fatalf("第 %d 步 Write 失败: %v", i, err)
			}
			puts += n

		default: // 30% 读取并比对
			got, err := db.Get([]byte(key))
			want, exists := golden[key]
			gets++

			if exists {
				hits++
				if err != nil {
					t.Fatalf("第 %d 步：Get(%q) 返回 %v，但 map 里有值 %q", i, key, err, want)
				}
				if string(got) != want {
					t.Fatalf("第 %d 步：Get(%q) = %q，期望 %q", i, key, got, want)
				}
			} else {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("第 %d 步：Get(%q) 应为 ErrNotFound，得到 (%q, %v)", i, key, got, err)
				}
			}
		}
	}

	// 收尾：把整个 key 空间完整比对一遍，防止上面的随机读漏掉了什么
	for i := 0; i < keySpan; i++ {
		key := fmt.Sprintf("key%05d", i)
		got, err := db.Get([]byte(key))
		want, exists := golden[key]

		if exists {
			if err != nil {
				t.Fatalf("最终比对：%q 应存在（值 %q），却返回 %v", key, want, err)
			}
			if string(got) != want {
				t.Fatalf("最终比对：%q = %q，期望 %q", key, got, want)
			}
		} else if !errors.Is(err, ErrNotFound) {
			t.Fatalf("最终比对：%q 不应存在，却返回 (%q, %v)", key, got, err)
		}
	}

	t.Logf("完成 %d 次操作：写入 %d、删除 %d、读取 %d（命中 %d）",
		ops, puts, deletes, gets, hits)
	t.Logf("map 里剩 %d 个 key，MemTable 里有 %d 条记录（含旧版本和墓碑），占用 %s",
		len(golden), db.mem.Count(), humanBytes(db.mem.Size()))
}

// TestSpaceAmplification 直观展示 LSM 的空间放大：
// 反复写同一个 key，MemTable 里的记录数会一直涨，因为旧版本不会被就地覆盖。
func TestSpaceAmplification(t *testing.T) {
	db := mustOpen(t)

	const n = 1000
	for i := 0; i < n; i++ {
		if err := db.Put([]byte("same-key"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("第 %d 次写入失败: %v", i, err)
		}
	}

	mustGet(t, db, "same-key", fmt.Sprintf("v%d", n-1))

	if db.mem.Count() != n {
		t.Errorf("MemTable 记录数 = %d，期望 %d（每次写入都是一个新版本）", db.mem.Count(), n)
	}
	t.Logf("同一个 key 写 %d 次，MemTable 里留下 %d 条记录，占用 %s —— 这就是空间放大，等 compaction 来清理",
		n, db.mem.Count(), humanBytes(db.mem.Size()))
}

// TestConcurrentReadsAndWrites 验证并发安全（配合 -race 跑）。
func TestConcurrentReadsAndWrites(t *testing.T) {
	db := mustOpen(t)

	const (
		writers = 4
		readers = 4
		each    = 500
	)
	done := make(chan struct{})

	for w := 0; w < writers; w++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < each; i++ {
				key := fmt.Sprintf("w%d-k%d", id, i)
				if err := db.Put([]byte(key), []byte("value")); err != nil {
					t.Errorf("并发写失败: %v", err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < each; i++ {
				key := fmt.Sprintf("w%d-k%d", id%writers, i)
				if _, err := db.Get([]byte(key)); err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("并发读失败: %v", err)
					return
				}
			}
		}(r)
	}
	for i := 0; i < writers+readers; i++ {
		<-done
	}

	// 所有写入都应该在
	for w := 0; w < writers; w++ {
		for i := 0; i < each; i++ {
			mustGet(t, db, fmt.Sprintf("w%d-k%d", w, i), "value")
		}
	}
}

func BenchmarkDBPut(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put([]byte(fmt.Sprintf("key%013d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBGet(b *testing.B) {
	db, err := Open(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 100000
	for i := 0; i < n; i++ {
		db.Put([]byte(fmt.Sprintf("key%013d", i)), []byte("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("key%013d", i%n))); err != nil {
			b.Fatal(err)
		}
	}
}
