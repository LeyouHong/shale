package shale

// Iterator 用来按 key 顺序遍历数据库。
//
// 典型用法：
//
//	it, err := db.NewIterator()
//	if err != nil { ... }
//	defer it.Close()
//
//	for it.Seek([]byte("user:")); it.Valid(); it.Next() {
//	    if !bytes.HasPrefix(it.Key(), []byte("user:")) {
//	        break // 前缀扫描：越界就停
//	    }
//	    fmt.Println(string(it.Key()), string(it.Value()))
//	}
//	if err := it.Error(); err != nil { ... }
//
// # 语义约定
//
//   - 迭代器看到的是【创建那一刻】的快照，之后的写入不影响它
//   - 只会看到有效数据：同一个 key 的多个版本只出最新的那个，
//     被删除的 key（墓碑）会被跳过 —— 调用方看不到 LSM 的内部机制
//   - Key() 和 Value() 返回的切片只在下次 Next/Seek 之前有效，
//     要保存必须自己复制
//   - 用完【必须】Close，否则它引用的 SSTable 文件无法被 compaction 删除
type Iterator interface {
	// Seek 定位到第一个 >= key 的位置。
	Seek(key []byte)

	// SeekToFirst 定位到最小的 key。
	SeekToFirst()

	// SeekToLast 定位到最大的 key。
	SeekToLast()

	// Next 移动到下一个 key。只有 Valid() 为 true 时才能调用。
	Next()

	// Prev 移动到上一个 key。只有 Valid() 为 true 时才能调用。
	Prev()

	// Valid 返回当前是否停在一个有效位置上。
	// 遍历越界或发生错误时返回 false。
	Valid() bool

	// Key 返回当前位置的 key。仅在 Valid() 为 true 时有意义。
	Key() []byte

	// Value 返回当前位置的 value。仅在 Valid() 为 true 时有意义。
	Value() []byte

	// Error 返回遍历过程中遇到的错误。
	// Valid() 变成 false 时应该检查它，以区分「正常走完」和「出错了」。
	Error() error

	// Close 释放迭代器持有的资源。
	Close() error
}
