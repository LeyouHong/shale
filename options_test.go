package shale

import "testing"

func TestDefaultOptions(t *testing.T) {
	o := DefaultOptions()
	if err := o.validate(); err != nil {
		t.Fatalf("默认配置自己都通不过校验: %v", err)
	}
	if o.MemTableSize != DefaultMemTableSize {
		t.Errorf("MemTableSize = %d，期望 %d", o.MemTableSize, DefaultMemTableSize)
	}
	if o.BloomBitsPerKey != 0 {
		// BloomBitsPerKey 的 0 表示关闭，是有意义的取值，
		// fillDefaults 不应该把它改掉
		t.Logf("注意：BloomBitsPerKey 默认为 %d", o.BloomBitsPerKey)
	}
}

func TestFillDefaultsOnlyTouchesZeroValues(t *testing.T) {
	o := &Options{
		MemTableSize:        1 << 20,
		L0CompactionTrigger: 8,
	}
	o.fillDefaults()

	if o.MemTableSize != 1<<20 {
		t.Error("已设置的 MemTableSize 被覆盖了")
	}
	if o.L0CompactionTrigger != 8 {
		t.Error("已设置的 L0CompactionTrigger 被覆盖了")
	}
	if o.MaxMemTables != DefaultMaxMemTables {
		t.Error("未设置的 MaxMemTables 没被填上默认值")
	}
}

// TestLevelMaxBytes 验证各层容量的计算 —— 这是最容易搞错的一块：
// L0 不参与「每层 10 倍」的关系，它由 MemTableSize × L0CompactionTrigger 决定。
func TestLevelMaxBytes(t *testing.T) {
	o := &Options{
		MemTableSize:        4 << 20, // 4MB
		L0CompactionTrigger: 4,
		LevelBaseSize:       8 << 20, // 8MB
		LevelSizeMultiplier: 10,
	}
	o.fillDefaults()

	cases := []struct {
		level int
		want  int64
	}{
		{0, 16 << 20},   // 4MB × 4 = 16MB，与 multiplier 无关
		{1, 8 << 20},    // LevelBaseSize
		{2, 80 << 20},   // L1 × 10
		{3, 800 << 20},  // L2 × 10
		{4, 8000 << 20}, // L3 × 10
	}
	for _, c := range cases {
		if got := o.LevelMaxBytes(c.level); got != c.want {
			t.Errorf("L%d 容量 = %d，期望 %d", c.level, got, c.want)
		}
	}
}

// TestLevelMaxBytesL0IsIndependent 单独强调一次：
// 改 LevelSizeMultiplier 不应该影响 L0。
func TestLevelMaxBytesL0IsIndependent(t *testing.T) {
	base := &Options{MemTableSize: 4 << 20, L0CompactionTrigger: 4}
	base.fillDefaults()
	l0 := base.LevelMaxBytes(0)

	other := &Options{MemTableSize: 4 << 20, L0CompactionTrigger: 4, LevelSizeMultiplier: 100}
	other.fillDefaults()

	if other.LevelMaxBytes(0) != l0 {
		t.Error("LevelSizeMultiplier 不应该影响 L0 的容量")
	}
	if other.LevelMaxBytes(2) == base.LevelMaxBytes(2) {
		t.Error("LevelSizeMultiplier 应该影响 L2 的容量")
	}
}

func TestOptionsValidate(t *testing.T) {
	valid := func() *Options {
		o := DefaultOptions()
		return o
	}

	t.Run("默认配置合法", func(t *testing.T) {
		if err := valid().validate(); err != nil {
			t.Errorf("默认配置应该合法: %v", err)
		}
	})

	t.Run("停写阈值不能小于合并阈值", func(t *testing.T) {
		o := valid()
		o.L0CompactionTrigger = 10
		o.L0StopWritesTrigger = 5
		if err := o.validate(); err == nil {
			t.Error("应该报错")
		}
	})

	t.Run("倍数至少为 2", func(t *testing.T) {
		o := valid()
		o.LevelSizeMultiplier = 1
		if err := o.validate(); err == nil {
			t.Error("应该报错")
		}
	})

	t.Run("布隆过滤器位数不能过大", func(t *testing.T) {
		o := valid()
		o.BloomBitsPerKey = 1000
		if err := o.validate(); err == nil {
			t.Error("应该报错")
		}
	})
}

func TestOptionsClone(t *testing.T) {
	orig := DefaultOptions()
	orig.MemTableSize = 12345

	c := orig.clone()
	c.MemTableSize = 999

	if orig.MemTableSize != 12345 {
		t.Error("修改副本影响到了原对象")
	}
}
