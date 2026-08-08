// Package memtable 是内存中的有序写缓冲，写满后冻结并刷成 SSTable。
//
// 对应 LSM 原理的：第 4 步 —— 在内存里排序
//
// 状态：尚未实现（见 DESIGN.md 第五节的里程碑拆解）。
package memtable
