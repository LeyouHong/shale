package shale

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "mydb") // 多级目录也应该能建出来

	db, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("目录没被创建: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("创建出来的不是目录")
	}
	if !filepath.IsAbs(db.Dir()) {
		t.Errorf("Dir() 应该返回绝对路径，得到 %q", db.Dir())
	}
}

func TestOpenNilOptionsUsesDefaults(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	if db.opts.MemTableSize != DefaultMemTableSize {
		t.Errorf("MemTableSize = %d，期望默认值 %d", db.opts.MemTableSize, DefaultMemTableSize)
	}
	if db.opts.L0CompactionTrigger != DefaultL0CompactionTrigger {
		t.Errorf("L0CompactionTrigger = %d，期望 %d",
			db.opts.L0CompactionTrigger, DefaultL0CompactionTrigger)
	}
}

// TestOpenClonesOptions 验证 Open 之后调用方再改 Options 不会影响已打开的 DB。
func TestOpenClonesOptions(t *testing.T) {
	opts := &Options{MemTableSize: 1 << 20}
	db, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	opts.MemTableSize = 999 // 从外面篡改

	if db.opts.MemTableSize != 1<<20 {
		t.Error("外部修改 Options 影响到了已打开的 DB")
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	cases := map[string]*Options{
		"停写阈值小于合并阈值":       {L0CompactionTrigger: 10, L0StopWritesTrigger: 4},
		"层级倍数过小":           {LevelSizeMultiplier: 1},
		"层数过少":             {MaxLevels: 1},
		"Block 大于 SSTable": {BlockSize: 1 << 20, SSTableSize: 1024},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			db, err := Open(t.TempDir(), opts)
			if err == nil {
				db.Close()
				t.Fatal("非法配置应该被拒绝")
			}
			if !errors.Is(err, ErrInvalidOptions) {
				t.Errorf("错误应该包裹 ErrInvalidOptions，得到 %v", err)
			}
		})
	}
}

func TestOpenReadOnlyRequiresExistingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := Open(missing, &Options{ReadOnly: true}); err == nil {
		t.Fatal("只读模式打开不存在的目录应该失败")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("第一次 Close 失败: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("重复 Close 应该是安全的，却返回 %v", err)
	}
}

// TestOperationsAfterClose 验证关闭之后所有操作都返回 ErrClosed，
// 而不是 panic 或者静默成功。
func TestOperationsAfterClose(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	db.Close()

	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get 应返回 ErrClosed，得到 %v", err)
	}
	if err := db.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Put 应返回 ErrClosed，得到 %v", err)
	}
	if err := db.Delete([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete 应返回 ErrClosed，得到 %v", err)
	}
	if err := db.Flush(); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush 应返回 ErrClosed，得到 %v", err)
	}
	if _, err := db.NewIterator(); !errors.Is(err, ErrClosed) {
		t.Errorf("NewIterator 应返回 ErrClosed，得到 %v", err)
	}
}

// TestWriteRejectsOversizedKey 验证超长 key 在真正写之前就被拦下。
func TestWriteRejectsOversizedKey(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	huge := make([]byte, MaxKeySize+1)
	if err := db.Put(huge, []byte("v")); !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("超长 key 应返回 ErrKeyTooLarge，得到 %v", err)
	}
	if _, err := db.Get(huge); !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("Get 超长 key 也应报错，得到 %v", err)
	}
}

func TestWriteRejectsOversizedValue(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{MemTableSize: 1024})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	big := make([]byte, 2048) // 比 MemTableSize 还大，永远刷不出去
	if err := db.Put([]byte("k"), big); !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("超大 value 应返回 ErrValueTooLarge，得到 %v", err)
	}
}

func TestWriteEmptyBatchIsNoop(t *testing.T) {
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	defer db.Close()

	if err := db.Write(NewBatch()); err != nil {
		t.Errorf("写空 Batch 应该直接成功，得到 %v", err)
	}
	if err := db.Write(nil); err != nil {
		t.Errorf("写 nil Batch 应该直接成功，得到 %v", err)
	}
}
