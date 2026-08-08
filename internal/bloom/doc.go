// Package bloom 实现布隆过滤器，让绝大多数无关文件不必真的读磁盘。
//
// 对应 LSM 原理的：第 9 步 —— 快速判断 key 不存在
//
// 状态：尚未实现（见 DESIGN.md 第五节的里程碑拆解）。
package bloom
