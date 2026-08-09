# shale — 需求分析与设计

> 一个用 Go 从零实现的 LSM Tree 存储引擎。
>
> **shale（页岩）**：层状沉积岩，由沉积物一层层压实而成——正对应 LSM 的分层下渗与 compaction 压实；
> 同时延续 LevelDB / RocksDB / Pebble 的「石头」命名传统。

---

## 一、定位与目标

### 1.1 一句话定位

> **一个可读性优先的嵌入式 KV 存储引擎，用来把 LSM Tree 彻底吃透。**

### 1.2 明确的目标

| 目标 | 具体含义 |
|---|---|
| **正确** | 已经返回成功的写入，进程被 `kill -9` 后重启必须还在 |
| **可读** | 每个模块能对照 LSM 原理的某一步；关键决策在代码里写清楚"为什么" |
| **可验证** | 每个里程碑都能跑起来、有测试证明它工作 |
| **零依赖** | 只用 Go 标准库。依赖越少，越能看清每一层到底在干什么 |

### 1.3 明确的非目标（很重要）

写下来是为了**防止范围蔓延**。以下内容这一版**不做**：

```
   ✗ 事务 / MVCC / 快照隔离       → 只保证单次操作和 WriteBatch 的原子性
   ✗ SQL 层、表、二级索引          → 这是上层的事，本项目只做 KV
   ✗ 网络服务、客户端协议          → 只做嵌入式库，import 进程序用
   ✗ 分布式、复制、一致性协议       → 单机单进程
   ✗ 列族（Column Family）        → 一个 DB 一个 key 空间
   ✗ 跑赢 RocksDB                 → 性能不是第一目标，正确和清晰才是
   ✗ 数据压缩（Snappy/ZSTD）      → 留到最后有余力再说
```

> 判断标准：**如果一个特性不能帮助理解 LSM 的核心机制，就不做。**

---

## 二、功能需求

### 2.1 对外 API

嵌入式库，只暴露一个包 `shale`：

```go
package shale

// ── 打开与关闭 ─────────────────────────────
func Open(dir string, opts *Options) (*DB, error)
func (db *DB) Close() error

// ── 单条操作 ───────────────────────────────
func (db *DB) Put(key, value []byte) error
func (db *DB) Get(key []byte) ([]byte, error)   // 不存在返回 ErrNotFound
func (db *DB) Delete(key []byte) error

// ── 批量原子写 ─────────────────────────────
type Batch struct{ ... }
func (b *Batch) Put(key, value []byte)
func (b *Batch) Delete(key []byte)
func (db *DB) Write(b *Batch) error             // 整批原子生效

// ── 范围扫描 ───────────────────────────────
type Iterator interface {
    Seek(key []byte)      // 定位到 >= key 的第一个
    SeekToFirst()
    Next()
    Valid() bool
    Key() []byte
    Value() []byte
    Close() error
}
func (db *DB) NewIterator() Iterator

// ── 运维 ──────────────────────────────────
func (db *DB) Flush() error                     // 强制把 MemTable 刷成 SSTable
func (db *DB) CompactAll() error                // 手动触发全量 compaction
func (db *DB) Stats() Stats                     // 各层文件数/大小，用于观察
```

**设计约定**：

```
   key、value 一律是 []byte —— 不做任何类型解释，编码是调用方的事
   key 允许为空吗？  → 允许（空 key 是合法的最小 key）
   value 允许为空吗？→ 允许（空 value ≠ 删除，删除有专门的墓碑）
   key/value 大小上限？→ 软限制：单条 value 不建议超过 MemTable 的 1/4
```

### 2.2 配置项

```go
type Options struct {
    MemTableSize        int64  // MemTable 多大触发 flush，默认 4MB（学习项目用小值方便观察）
    MaxMemTables        int    // 最多几个 MemTable 排队，默认 2
    L0CompactionTrigger int    // L0 攒几个文件触发 compaction，默认 4
    L0StopWritesTrigger int    // L0 超过几个文件停写，默认 12
    LevelBaseSize       int64  // L1 的总容量，默认 8MB
    LevelSizeMultiplier int    // 从 L1 起每层放大倍数，默认 10
    SSTableSize         int64  // 单个 SSTable 目标大小，默认 2MB
    BlockSize           int    // SSTable 内 Data Block 大小，默认 4KB
    BloomBitsPerKey     int    // 布隆过滤器每 key 几 bit，0 表示关闭，默认 10
    BlockCacheSize      int64  // Block 缓存大小，默认 8MB
    SyncWAL             bool   // 每次写是否 fsync，默认 false
}
```

> 默认值刻意调小（MemTable 4MB 而不是 RocksDB 的 64MB）——
> **学习项目要让 flush 和 compaction 频繁发生，才看得到现象。**

### 2.3 语义定义

必须先把语义定死，否则测试没法写：

| 场景 | 期望行为 |
|---|---|
| `Get` 一个没写过的 key | 返回 `ErrNotFound` |
| `Get` 一个被 `Delete` 过的 key | 返回 `ErrNotFound` |
| 同一个 key 写两次 | 后写的生效 |
| `Delete` 一个不存在的 key | 成功（不报错），留下一个墓碑 |
| `Put` 空 value | 成功，`Get` 返回长度为 0 的 slice，**不是** `ErrNotFound` |
| `Write(batch)` 中途失败 | 整批不生效（要么全成功要么全失败） |
| `Put` 返回成功后立刻 `kill -9` | 重启后 `Get` 必须能读到（`SyncWAL=true` 时保证） |
| Iterator 遍历中有并发写入 | 迭代器看到的是创建那一刻的快照，不受影响 |

---

## 三、非功能需求

### 3.1 正确性（第一优先级）

```
   崩溃安全：任何时刻 kill -9，重启后
             · 所有已返回成功的写入都在（SyncWAL=true）
             · 数据文件不会损坏到无法打开
             · 未写完的 SSTable / WAL 尾部残片能被识别并跳过

   数据完整性：每个 Block、每条 WAL 记录都带 CRC32 校验
```

### 3.2 性能（不是第一目标，但要有底线）

在一台普通笔记本上，1000 万条 16 字节 key / 100 字节 value：

| 指标 | 底线目标 | 说明 |
|---|---|---|
| 顺序写吞吐 | ≥ 10 万 ops/s | 只写 WAL + 内存，不该太慢 |
| 随机点查（命中缓存） | ≥ 10 万 ops/s | |
| 随机点查（读磁盘） | ≥ 1 万 ops/s | 有布隆过滤器兜底 |
| 查一个不存在的 key | ≥ 5 万 ops/s | **专门测这个**，验证布隆过滤器真的生效了 |
| 写放大 | ≤ 30 倍 | 用 stats 统计实际写入字节 / 用户写入字节 |

> 达不到不算失败，但**要能解释为什么**——那本身就是学习成果。

### 3.3 代码质量

```
   · 每个 internal 包顶部有一段 doc.go 或包注释，说明它对应 LSM 的哪一步
   · 关键取舍在代码里用注释写清楚「为什么这么做」，而不只是「做了什么」
   · 单元测试覆盖每个包；DB 层有端到端测试
   · 不引入任何第三方依赖（go.mod 里 require 段为空）
   · 通过 go vet、gofmt；开启 -race 跑测试
```

---

## 四、核心设计决策

每个决策记录：**选项 → 选择 → 理由**。这一节是整个文档最有价值的部分。

### D1：MemTable 用什么数据结构

| 选项 | 优点 | 缺点 |
|---|---|---|
| **跳表 SkipList** | 有序、O(log n)、并发友好、实现简单 | 指针多，内存开销略大 |
| 红黑树 | 内存紧凑 | 旋转逻辑复杂，并发难做 |
| 有序数组 + 二分 | 最省内存，缓存友好 | 插入要搬数据，O(n) |

**选择：跳表**，自己实现（不用第三方库）。

**理由**：这是 LSM 的经典选择，且亲手写一遍跳表本身就是目标之一。第一版做**单写者 + 读写锁**，
不上无锁实现——无锁跳表的正确性验证成本极高，留到 M9 再考虑。

### D2：内部 key 怎么编码

这是**最难改**的决策，必须一次定对。

```
   内部 key = 用户 key + 序列号 + 类型

   ┌──────────────┬───────────────┬──────────┐
   │  user key    │  seq (7 字节)  │ kind(1B) │
   └──────────────┴───────────────┴──────────┘
                    单调递增的全局序号   0=删除(墓碑)
                                        1=写入

   排序规则：先按 user key 升序，user key 相同时按 seq 【降序】
```

**为什么要有 seq**：同一个 key 会有多个版本散落在各层，必须能区分新旧。
**为什么 seq 降序排**：这样同一个 key 的**最新版本排在最前面**，
查找时定位到第一个匹配项就是答案，不用继续往后扫。

```
   例：key="a" 先后写了 3 次（seq=5,9,12），第 12 次是删除

   内部 key 的排序结果：
   ("a", 12, 删除)   ← 最新，Get 定位到这个 → 返回 NotFound ✓
   ("a",  9, 写入)
   ("a",  5, 写入)
```

**这个设计同时解决了 Batch 的原子性**：一个 batch 内所有操作共享同一个 seq 起始值。

### D3：WAL 的记录格式

参考 LevelDB 的分块日志格式（这是被验证过的成熟设计）：

```
   WAL 文件按 32KB 分块，一条记录可能跨块

   ┌──────────┬────────┬────────┬──────────────────┐
   │ CRC32 4B │ Len 2B │ Type 1B│  Payload         │
   └──────────┴────────┴────────┴──────────────────┘
                                Type: FULL / FIRST / MIDDLE / LAST

   为什么分块：崩溃时文件尾部可能是半条记录。
              分块 + CRC 能让恢复时准确识别"读到哪里为止是完好的"
```

**决策**：`SyncWAL` 默认 `false`（只写 page cache，不 fsync）。
理由：fsync 会让写入慢一个数量级；学习项目默认追求可观察的吞吐，
但**必须提供 `true` 选项并测试它真的能防崩溃**。

### D4：SSTable 文件格式

```
   ┌─────────────────────────────────────────┐
   │  Data Block 1   (默认 4KB，内部 key 有序) │
   │  Data Block 2                            │
   │  ...                                     │
   ├─────────────────────────────────────────┤
   │  Filter Block   (布隆过滤器位图)          │
   ├─────────────────────────────────────────┤
   │  Index Block    (每个 Data Block 的      │
   │                  最大 key + 偏移量)       │
   ├─────────────────────────────────────────┤
   │  Footer (固定 48B)                       │
   │    · Index Block 的偏移量和长度           │
   │    · Filter Block 的偏移量和长度          │
   │    · Magic Number（识别文件类型）         │
   └─────────────────────────────────────────┘

   每个 Block 尾部带 CRC32
```

**Block 内部用前缀压缩**（restart point 机制）还是直接存完整 key？
→ **第一版直接存完整 key**，简单优先。前缀压缩留作 M10 的优化练习。

### D5：Compaction 策略

| 选项 | 说明 |
|---|---|
| **Leveled**（选） | L0 重叠，L1+ 层内不重叠，每层 10 倍。读放大低 |
| Tiered | 写放大低但读放大高 |

**选择：Leveled**，分两阶段实现：

```
   M6：先做最笨的版本 —— L0 满了就把 L0 全部 + L1 全部合并成新的 L1
       目的是先把归并逻辑跑通

   M7：再做真正的 Leveled —— 从 L(n) 挑一个文件，
       找出 L(n+1) 中所有重叠的文件，只合并这些
```

**触发条件**：

```
   L0：文件数 >= L0CompactionTrigger（默认 4）
   L1+：该层总大小 > 该层容量上限
   同时满足多个时，优先处理 score 最高的（超出容量比例最大的）
```

**墓碑清理规则**：只有 compaction 输出到**最底层**时，墓碑才能真正丢弃。

### D6：并发模型

```
   第一版（M1~M8）：
   ┌────────────────────────────────────────────┐
   │  写：全局互斥锁，同一时刻只有一个写者          │
   │  读：读写锁的读锁，多读者并发                 │
   │  Compaction：后台 goroutine，通过 channel    │
   │              接收触发信号                    │
   └────────────────────────────────────────────┘

   理由：先保证正确。LSM 的难点在数据结构和文件格式，
        不在并发优化。等一切跑通了再谈无锁。

   M9 再考虑：MemTable 无锁跳表、批量写合并（group commit）
```

**版本管理必须一开始就设计对**——因为 compaction 会在后台删文件，
而前台的 Iterator 可能还在读它：

```
   引入 Version（当前有哪些文件、分布在哪些层）+ 引用计数
   · Iterator 创建时持有一个 Version 的引用
   · Compaction 产生新 Version，旧 Version 引用归零后才真正删文件
   · 这也顺便实现了「Iterator 看到的是创建时刻的快照」
```

### D7：元数据怎么持久化（Manifest）

```
   问题：重启后怎么知道有哪些 SSTable、各自在哪一层？

   方案：MANIFEST 文件，记录版本变更日志（VersionEdit）
        · 新增文件 / 删除文件 / 层级变化 / 下一个可用的文件号 / 下一个 seq
        · 格式复用 WAL 的分块格式
        · CURRENT 文件指向当前生效的 MANIFEST

   为什么不扫描目录？—— 目录里可能有 compaction 中途留下的垃圾文件，
                      必须有一份权威记录说明"哪些文件是当前有效的"
```

---

## 五、里程碑拆解

**核心原则：每个里程碑结束时，项目都能编译、能跑、有测试。** 绝不写一大坨最后才调试。

| 里程碑 | 做什么 | 验收标准 |
|---|---|---|
| **M0** | 项目骨架、`go.mod`、Options、错误定义、测试框架 | `go test ./...` 通过（空测试） |
| **M1** | 跳表 + MemTable，纯内存的 Put/Get/Delete | 对拍测试：随机 10 万次操作，结果与 Go `map` + 排序完全一致 |
| **M2** | WAL 写入 + 重启恢复 | 写 1 万条 → 模拟崩溃 → 重开 → 数据全在；尾部截断半条记录仍能正确恢复 |
| **M3** | SSTable 写入与读取（单文件），内部 key 编码 | MemTable 刷成文件 → 关掉 → 重开只从文件读 → 数据一致 |
| **M4** | Manifest + Version + 多文件管理 | 多次 flush 产生多个文件；重启后能正确加载文件列表 |
| **M5** | Iterator：单文件迭代器 + 多路归并迭代器 | 范围扫描结果有序、去重（同 key 只出最新版本）、正确跳过墓碑 |
| **M6** | 最简单的 Compaction（L0+L1 全量合并） | compaction 后文件数下降、旧版本被清理、数据不变 |
| **M7** | 真正的 Leveled Compaction + 分层 | 灌 100MB 数据，观察各层文件数符合 10 倍关系；`Stats()` 能打印层级分布 |
| **M8** | 布隆过滤器 + Block Cache | **专项测试：查 10 万个不存在的 key，统计实际磁盘读次数应接近 0** |
| **M9** | 并发安全 + `-race` 测试 + 压测 | 10 个 goroutine 并发读写不出错；跑出 3.2 节的性能数字 |
| **M10** | 可选优化：前缀压缩、压缩算法、group commit | — |

**M1 的对拍测试是整个项目的安全网**，值得多花时间做好：

```go
// 思路：用 Go 原生 map 作为"标准答案"，随机操作序列同时打给两边，比对结果
for i := 0; i < 100000; i++ {
    switch rand.Intn(3) {
    case 0: k, v := randKV(); db.Put(k, v);  golden[string(k)] = v
    case 1: k := randKey();   db.Delete(k);  delete(golden, string(k))
    case 2: k := randKey()
            got, err := db.Get(k)
            want, ok := golden[string(k)]
            assertEqual(got, err, want, ok)   // 任何不一致立刻暴露
    }
}
```

这个测试在 M1 建好之后，**后面每个里程碑都复用它**——加了 WAL 跑一遍、加了 SSTable 跑一遍、
加了 compaction 再跑一遍。任何一步引入的 bug 都会被它抓住。

---

## 六、项目结构

```
shale/
├── go.mod                    // module github.com/leyouhong/shale，无第三方依赖
├── README.md
├── DESIGN.md                 // 本文档
├── db.go                     // 对外 API：Open/Put/Get/Delete/Write/NewIterator
├── options.go                // Options 及默认值
├── batch.go                  // WriteBatch
├── errors.go                 // ErrNotFound 等
├── db_test.go                // 端到端测试 + 对拍测试
│
├── internal/
│   ├── skiplist/             // 【对应原理第 4 步】跳表
│   ├── memtable/             // 【第 4 步】MemTable，包一层跳表 + 内部 key 编码
│   ├── ikey/                 // 内部 key 的编解码（user key + seq + kind）
│   ├── wal/                  // 【第 5 步】预写日志，分块格式 + CRC
│   ├── sstable/              // 【第 3 步】SSTable 的 writer / reader / 格式定义
│   ├── bloom/                // 【第 9 步】布隆过滤器
│   ├── cache/                // Block Cache（LRU）
│   ├── iterator/             // 迭代器接口 + 多路归并
│   ├── version/              // 版本管理、Manifest、层级元数据、引用计数
│   └── compaction/           // 【第 6~8 步】compaction 策略与执行
│
└── cmd/
    └── shalectl/             // 调试工具，仿 ldb：dump SSTable、看层级分布
```

> 用 `internal/` 是有意的——Go 会禁止外部包 import 它，
> 正好表达"这些都是实现细节，对外只有根包那几个 API"。

---

## 七、测试策略

| 类型 | 做什么 |
|---|---|
| **对拍测试** | 与 Go `map` 比对，M1 建好后每个里程碑都复用（最重要） |
| **单元测试** | 每个 internal 包独立测：跳表的增删查、CRC 校验、bloom 假阳性率 |
| **崩溃测试** | 在各个时机强制中断（写 WAL 中途、flush 中途、compaction 中途），验证重启恢复 |
| **文件截断测试** | 手动把 WAL / SSTable 尾部截断若干字节，验证能识别并跳过残片 |
| **不变量检查** | 提供 `checkInvariants()`：L1+ 各层内文件 key 范围不重叠、Manifest 与磁盘文件一致 |
| **基准测试** | `go test -bench`，覆盖 3.2 节的每个指标 |
| **竞态检测** | `go test -race ./...`，M9 起必须全绿 |

---

## 八、已知难点与风险

| 难点 | 说明 | 应对 |
|---|---|---|
| **文件生命周期管理** | compaction 删文件时可能有 Iterator 正在读 | D6 的 Version 引用计数，一开始就设计对，别后补 |
| **崩溃恢复的边界情况** | 半条 WAL、半个 SSTable、Manifest 写到一半 | 每个 block 带 CRC；Manifest 用追加日志而非覆盖写 |
| **seq 号的分配与持久化** | 重启后 seq 必须接着来，否则新写入会被误判为旧版本 | seq 记在 Manifest 里；恢复时取 max(manifest, WAL 里的最大值) |
| **墓碑什么时候能删** | 提前删会让旧数据"复活" | 只在输出到最底层时才丢弃 |
| **Iterator 的多路归并** | 要正确处理同 key 多版本、墓碑跳过 | 先写好单元测试再写实现 |
| **范围蔓延** | 越写越想加功能 | 严格对照 1.3 节的非目标清单 |

---

## 九、回顾：实际做下来和设计文档的出入

M0~M9 已全部完成。有几处实现和这份文档最初的设计不一样，记在这里：

| 当初的设计 | 实际做法 | 原因 |
|---|---|---|
| compaction 放 `internal/compaction` | 放在根包的 `compaction.go` | 它需要 vs / opts / 句柄缓存等大量 DB 上下文，拆成子包反而要来回传参 |
| seq 只记在 Manifest | M3 先用"扫 SSTable 求 MaxSeq"顶了一版，M4 才换成 Manifest | M3 时 Manifest 还没有，但 flush 会删 WAL，seq 必须有别的来源 |
| 迭代器直接引用 MemTable | 复制成只读快照 | 跳表不支持并发读写，而迭代器可能活很久 |
| Iterator 接口含 `Prev` / `SeekToLast` | 去掉了 | 反向遍历代价高（跳表无反向指针、SSTable 块单向链）且暂无场景，留着会变成"有方法但会 panic"的陷阱 |
| 只有一把 `db.mu` | 额外加了 `tablesMu` | 文件句柄缓存在【读路径上会被写】（懒加载），读锁挡不住 |
| 需要 smallest_snapshot 机制 | **不需要** | 「不可变文件 + 版本引用计数」已经保证了正在读的迭代器不受影响 |

最后一条是设计上的意外收获：一开始担心 compaction 丢弃旧版本会破坏长时间运行的迭代器，
准备照抄 LevelDB 的 smallest_snapshot。实际做下来发现 M4 建的引用计数已经解决了 ——
compaction 从不修改现有文件，旧读者读的还是旧文件，内容一字节没变。

## 十、这份清单起作用了吗

第八节列的"已知难点"里，有三条在实现时确实踩了：

- **seq 的分配与持久化** —— M3 漏了，表现为"flush 后重启数据全查不到"
- **文件生命周期管理** —— M9 漏了迭代器 Close 的加锁，表现为"文件被误删"
- **范围蔓延** —— 靠 1.3 节的非目标清单挡住了几次想加功能的冲动

清单没能阻止我犯错，但每次出问题时，一看现象就知道该往哪查。这大概就是它的价值。

## 十一、如果要继续

按价值排序：

```
   1. group commit          多个并发写攒一批只 fsync 一次
                            —— 当前 SyncWAL=true 时只有 465 ops/s，这是最大的短板
   2. 无锁跳表               去掉迭代器复制 MemTable 的开销
   3. SSTable 块内前缀压缩    key 有大量公共前缀时能省不少空间
   4. 压缩算法（LZ4/ZSTD）    冷数据层用重压缩
   5. Partitioned Index      单文件索引块过大时分片加载
```
