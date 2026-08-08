# shale

用 Go 从零实现的 **LSM Tree 存储引擎**，零第三方依赖。

> **shale**（页岩）是层状沉积岩，由沉积物一层层压实而成——正对应 LSM 的分层下渗与
> compaction 压实；同时延续 LevelDB / RocksDB / Pebble 的「石头」命名传统。

这是一个**学习向**的项目：目标是把 LSM 彻底吃透，代码可读性优先于性能。
完整的需求分析、设计决策和里程碑拆解见 **[DESIGN.md](DESIGN.md)**。

---

## 当前进度

| 里程碑 | 内容 | 状态 |
|---|---|---|
| **M0** | 骨架、Options、错误定义、**内部 key 编码**、**Batch 格式** | ✅ 完成 |
| **M1** | **跳表 + MemTable**（纯内存的 Put/Get/Delete） | ✅ 完成 |
| **M2** | **WAL + 崩溃恢复** | ✅ 完成 |
| M3 | SSTable 读写 | ⬜ |
| M4 | Manifest + Version + 多文件管理 | ⬜ |
| M5 | Iterator + 多路归并 | ⬜ |
| M6 | Compaction（简单全量合并） | ⬜ |
| M7 | Leveled Compaction + 分层 | ⬜ |
| M8 | 布隆过滤器 + Block Cache | ⬜ |
| M9 | 并发安全 + 压测 | ⬜ |

M0 刻意**先把两个最难改的决策落地**——内部 key 编码和 Batch 二进制格式。
它们是纯函数、不依赖存储，现在做掉，后面所有模块就不用返工。

M1 打通了读写路径。M2 加上 WAL，数据能活过重启——包括 `kill -9`。

**当前的限制**：还没有 SSTable，所以 MemTable 和 WAL 都会一直涨，
重启要重放全部日志。M3 让数据能落盘之后才会解决。

当前实测：

| 场景 | 耗时 | 吞吐 |
|---|---|---|
| 写入（M1，纯内存） | 546 ns/op | 183 万 ops/s |
| 写入（M2，WAL 不 sync） | 3.8 μs/op | 26 万 ops/s |
| 写入（M2，`SyncWAL: true`） | **2149 μs/op** | **465 ops/s** |
| 读取 | 401 ns/op | 249 万 ops/s |

最后一行是这个项目目前最值得玩味的数字：**开了 fsync 之后慢了 567 倍**。

```
   SyncWAL=false   写完就返回，数据在操作系统的页缓存里
                   → 进程崩溃不丢，【机器断电会丢】

   SyncWAL=true    每次写都等磁盘确认落盘
                   → 断电也不丢，但每次都要等一次真实的磁盘往返
```

这正是 WAL 那一节说的取舍。真实数据库靠 **group commit**（多个并发写攒成一批、
只 fsync 一次）来摊薄这个成本，本项目留到 M9。

---

## 目标 API

```go
db, err := shale.Open("/tmp/mydb", nil)
defer db.Close()

db.Put([]byte("hello"), []byte("world"))
v, err := db.Get([]byte("hello"))
db.Delete([]byte("hello"))

// 原子批量写
b := shale.NewBatch()
b.Put([]byte("k1"), []byte("v1"))
b.Delete([]byte("k2"))
db.Write(b)

// 范围扫描
it, _ := db.NewIterator()
defer it.Close()
for it.Seek([]byte("user:")); it.Valid(); it.Next() {
    fmt.Println(string(it.Key()), string(it.Value()))
}

// 观察 LSM 的内部状态
fmt.Println(db.Stats())
```

---

## 代码结构

```
shale/
├── db.go            对外 API
├── options.go       配置项与各层容量计算
├── batch.go         原子批量写（其二进制形式即 WAL 记录格式）
├── iterator.go       迭代器接口
├── stats.go         运行状态，观察 LSM 行为的窗口
│
└── internal/
    ├── ikey/        内部 key 编码 —— user key + seq + kind      ✅
    ├── skiplist/    跳表                          【原理第 4 步】✅
    ├── memtable/    内存有序写缓冲                 【第 4 步】    ✅
    ├── wal/         预写日志                      【第 5 步】    ✅
    ├── sstable/     磁盘上的有序文件               【第 3 步】
    ├── bloom/       布隆过滤器                     【第 9 步】
    ├── cache/       Block 缓存
    ├── iterator/    内部迭代器 + 多路归并           【第 6 步】
    ├── version/     版本管理、Manifest、引用计数    【第 8 步】
    └── compaction/  合并策略与执行                 【第 6~8 步】
```

括号里的「第 N 步」对应 LSM 原理笔记里的推导步骤，方便代码和原理对着看。

---

## 两个核心设计

### 内部 key 编码

所有模块存的都不是用户 key，而是**内部 key**：

```
┌──────────────────┬────────────────────────────────┐
│   user key       │  trailer (8 字节)               │
│   任意长度        │  = seq(7字节) << 8 | kind(1字节) │
└──────────────────┴────────────────────────────────┘

排序：先按 user key 升序，相同时按 seq 【降序】
```

`seq` 降序是关键——同一个 key 的**最新版本自动排在最前**，
查找时定位到第一个匹配项就是答案，不用继续扫。

```
("a", 12, 墓碑)   ← Get 定位到这条 → 返回 NotFound ✓
("a",  9, 写入)
("a",  5, 写入)
```

### Batch 的二进制形式就是 WAL 记录

```
┌───────────────┬──────────────┬──────────────────────┐
│ seq (8B, LE)  │ count (4B,LE)│  record × count       │
└───────────────┴──────────────┴──────────────────────┘
```

不需要二次转换：`batch.Bytes()` 直接写进 WAL，恢复时 `batch.Load()` 读回来。
整批共享同一个起始 seq，第 i 条的序号是 `seq+i`——**原子性在 key 编码层面就体现了**。

---

## 开发

```bash
go test ./...              # 跑全部测试
go test -race ./...        # 竞态检测
go test -bench=. ./...     # 基准测试
go vet ./... && gofmt -l . # 静态检查与格式
```

约束：

- **不引入任何第三方依赖**——依赖越少，越能看清每一层在干什么
- 每个 `internal` 包的注释说明它对应 LSM 原理的哪一步
- 关键取舍在代码里写清楚「为什么」，而不只是「做了什么」
