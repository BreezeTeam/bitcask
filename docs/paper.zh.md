# Bitcask Research KV Engine — 日志结构 KV 的写路径、耐久、崩溃一致性、compaction、KV 分离与自调优实验研究

> **v1.0.0 已完成。** 六条轨道均具备机制 · 不变量 · 测试 · 实测表 · 局限。  
> 总览：[`../README.zh.md`](../README.zh.md) · English paper：[`paper.md`](paper.md)

## 一眼结论

测量：Apple M3 Max / darwin/arm64 / Go 1.26.2 / `-count=5`。原始数据：[`../results/`](../results/)。

| 发现 | 数字 |
| --- | --- |
| 小 value put，无 sync | ≈ **39 µs** |
| 同上，每提交 sync | ≈ **4.4 ms**（受 fsync 束缚） |
| 16 writer：group commit vs every-commit | 约 **14×**（636 µs vs 8.87 ms） |
| budgeted vs static merge（摊销） | ≈ **44 µs** vs ≈ **1.74 ms** |
| 64 KB put：inline → separated | ≈ **1.88 ms → 914 µs** |
| 真实 group-epoch 崩溃恢复 + oracle | ≈ **37 ms/op** |

下文给出问题、机制与威胁效度；分轨细节见 [`tracks/`](tracks/)（英文）。

## 摘要

Bitcask 是简单、成熟的日志结构 KV：追加写数据日志，内存索引指向每个 key 的最新 live 记录。正因简单，它适合**隔离地**研究存储引擎权衡——不必让 LSM 的 compaction 机器盖过信号。本项目在可工作的 Go Bitcask 引擎上沿六条研究轨道扩展：写路径分配、耐久流水线、崩溃一致性探索、SLO 感知 compaction、生命周期 KV 分离、自治自调优。每条轨道有显式机制、正确性不变量、确定性测试（含真实子进程崩溃恢复）与可复现基准。调用方使用

`github.com/BreezeTeam/bitcask/engine`（包名仍为 `bitcask`），

盘上格式在轨道间保持稳定；实验特性均为 opt-in 并写明局限。贡献不在于新算法，而在于对代价与收益的**可测量、诚实、可复现**交代。

## 研究问题

- **RQ1（写路径）** 单次写延迟来自哪里？哪些成本相对 entry 固定，哪些随 value 变大？batch API 与 buffer/arena 复用能否在不破坏对象生命周期安全的前提下降低分配？
- **RQ2（耐久）** 合并 fsync（group commit）与自适应 sync 能降多少耐久成本，且不发明「强于配置」的耐久语义？
- **RQ3（崩溃一致性）** 崩溃结果能否变得**确定且可检验**——语义故障点 + 未干净关闭的真实进程退出——使「未提交事务 reopen 后不可见」成为测过的不变量而非口号？
- **RQ4（compaction）** 能否用**测得的**垃圾/热度与延迟预算驱动 merge，前台延迟代价如何？
- **RQ5（KV 分离）** value 进入分段 value log 后 put/get 代价如何？生命周期放置 + 指针安全 GC 能否回收空间且不留下悬空引用？
- **RQ6（自治）** 引擎能否检测负载相位并在**永不削弱显式耐久**的护栏下推荐/应用策略，观测开销多大？

## 贡献

1. **内聚引擎**位于 [`engine/`](../ARCHITECTURE.zh.md)（`package bitcask`），私有叶子在 `internal/`，跨六轨保持格式兼容。  
2. 每轨**带不变量的机制**，有确定性测试；耐久/崩溃轨含真实 `os.Exit` 中途退出再 reopen 的 oracle 检验。  
3. **复现脚手架**（`make check` / `make bench`）与提交的原始数据 [`../results/`](../results/)，headline 数字可用一条命令在读者机器上重跑。  
4. **诚实的局限姿态**：各轨写明 v1.0 未覆盖之处（连续后台 worker、完整文件系统丢失仿真、闭环自治收益等），未测者不写成保证。

## 系统概览

写路径：`DB.Update/PutBatch → Begin → Tx.Put → Commit → append → encode → WriteAt → index → sync`。可见性由 **committed marker** 门控：仅当事务最后一条记录以 `Committed` 写入且 id 登记后才可见；恢复忽略未提交记录。在此之上：sync 策略与 group-commit 协调器（B）；语义/元数据故障注入与崩溃 oracle（C）；测得的 compaction 观测、选择器与带预算 merge（D）；分段 value log、生命周期放置与 CRC manifest 上的停世界 GC（E）；负载相位检测与受护栏策略应用（F）。子系统文件地图见 [`../ARCHITECTURE.zh.md`](../ARCHITECTURE.zh.md)。

## 六条轨道

| 轨 | 问题 | 机制 | 关键不变量 | 章节 |
| --- | --- | --- | --- | --- |
| A 写路径 | RQ1 | batch API、encode/arena 复用、owned pending | `Put` 后调用方改 slice 不能改变已提交/索引/编码字节 | [write-path](tracks/write-path.md) |
| B 耐久 | RQ2 | None/Every/Group/Adaptive，value-log 先于 main sync | group commit 合并共享 fsync，不超出配置耐久 | [durability-pipeline](tracks/durability-pipeline.md) |
| C 崩溃 | RQ3 | 语义+元数据故障点、子进程突变退出、oracle | 未提交/撕裂 marker 的事务 reopen 后不可见 | [crash-explorer](tracks/crash-explorer.md) |
| D compaction | RQ4 | 测得垃圾/热度、选择器、延迟预算 merge | 字节预算下仍保持最新 live 记录可见性 | [slo-compaction](tracks/slo-compaction.md) |
| E KV 分离 | RQ5 | 分段 value log、生命周期放置、指针安全 GC | GC 不让 live key 指向已回收 value | [lifecycle-kv-separation](tracks/lifecycle-kv-separation.md) |
| F 自治 | RQ6 | 相位检测、策略推荐、受护栏 apply | apply 不改 sync 策略、不削弱耐久 | [autonomous-storage](tracks/autonomous-storage.md) |

完成矩阵：[`ledger/research-tracks-2026.zh.md`](ledger/research-tracks-2026.zh.md)。

## Headline 结果

环境：**Apple M3 Max, darwin/arm64, Go 1.26.2, APFS SSD**，`-benchmem -count=5`。  
sync/fsync/子进程的绝对 `ns/op` 绑设备；`allocs/op` / `B/op` 更可移植。原始输出：[`../results/`](../results/)。

| 轨 | 代表基准 | Measured (ns/op) | B/op | allocs/op | 解读 |
| --- | --- | ---: | ---: | ---: | --- |
| A | `TxCommitSingleEntryBaseline/32B/sync=false` | 39,134 | 865 | 15 | 小 value 固定成本主导 |
| A | `…/sync=true` | 4,407,716 | 836 | 15 | fsync（~4.4 ms）主导端到端 |
| B | group vs every，16 writers | 636,271 vs 8,873,376 | 985 | 19 | 并发下合并 fsync ~14× |
| C | `GroupCommitSubprocessRecovery` | 36,938,783 | 29,664 | 290 | 真崩溃 + reopen + oracle |
| D | budgeted vs static merge | 43,823 vs 1,737,462 | 1,383 | 19 | budgeted 接近基线；static 很贵 |
| E | `KVSeparationPut` 64KB sep vs inline | 913,702 vs 1,882,589 | 66,408 | 16 | 大 value put 约减半 |
| F | observation on vs off | 52,466 vs 39,237 | 772 | 15 | 检测约 +13 µs/commit |

更全的表在各轨 *Benchmarks*；方法见 [`methodology/benchmarks.md`](methodology/benchmarks.md)。

## 结论

在所述脚手架下（单机、合成微基准、opt-in 特性）：

1. **写路径（RQ1）** 小 value 固定成本主导；每提交 sync 把延迟推进 fsync 毫秒区。encode 复用与 batch 降低可移植分配成本，且不破坏所有权安全。  
2. **耐久（RQ2）** group commit 在并发下约 14× 受益于合并 sync；不发明强于配置的耐久。adaptive sync 用有界延迟窗口换更低 sync 率。  
3. **崩溃（RQ3）** 语义故障点 + 真 `os.Exit` + reopen，使「未提交不可见」在有界崩溃模型内可 oracle 检验——不是完整掉电仿真器。  
4. **compaction（RQ4）** 逻辑字节预算使前台代价接近 no-merge；无界 static merge 摊销昂贵。连续重叠后台 compaction 不在 v1.0 范围。  
5. **KV 分离（RQ5）** 大 value 分离约减半 put，get 接近 inline；指针安全 GC 由分阶段 manifest 与崩溃测试覆盖。  
6. **自治（RQ6）** 相位检测与受护栏推荐开销可测且小；apply 不削弱显式 sync。不声称闭环 QoS 收益。

这些是本产物的**局部可复现发现**，不是可移植绝对延迟或生产 SLO。范围边界见各轨 *Limitations* 与下文威胁效度。

## 复现

```bash
make check      # gofmt + go vet + go test + race + whitespace
make bench      # 六轨 → results/track-*.txt  (COUNT=5)
make bench-a    # 单轨 a…f
```

环境与可移植性：[`../results/README.md`](../results/README.md)。

## 威胁效度与局限

- **单机测量** 非多租户 / 多盘 / 服务器剖面标定。  
- **耐久数字绑设备** 勿跨硬件搬运绝对延迟。  
- **有界崩溃模型** 语义/元数据故障 + 突变退出，非完整掉电或任意 FS 损坏。  
- **非连续后台** merge / value-log GC 为触发式，非 v1.0 连续 worker。  
- **自治以观测为主** apply 受护栏且默认关；当前 harness 测检测开销，非已证闭环收益。

任何耐久或崩溃保证，都以链接测试实际覆盖为准。
