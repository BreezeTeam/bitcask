# Bitcask Research KV Engine

Go 实现的 Bitcask 风格日志结构 KV 存储，以**研究实验**形式组织：六条实验轨道，每条都有研究问题、机制、正确性不变量、测试与实测结果。

**中文入口：** 本文 · 论文 [`docs/paper.zh.md`](docs/paper.zh.md)  
**English:** [`README.md`](README.md) · [`docs/paper.md`](docs/paper.md)

| 文档 | 内容 |
| --- | --- |
| [docs/paper.zh.md](docs/paper.zh.md) | 摘要、问题、headline 结果、结论 |
| [docs/tracks/](docs/tracks/) | 分轨章节（机制 / 测试 / 表 / 局限；正文目前以英文为主） |
| [docs/methodology/](docs/methodology/) | 测量与格式解读 |
| [docs/ledger/research-tracks-2026.zh.md](docs/ledger/research-tracks-2026.zh.md) | 六轨完成矩阵与跨轨不变量 |
| [ARCHITECTURE.zh.md](ARCHITECTURE.zh.md) | 源码树地图 |
| [results/](results/) | 表格背后的原始 `go test -bench` 输出 |

**状态：v1.0.0（已完成）。**

> 这不是生产级多租户数据库。耐久、compaction、value log、自治等实验能力均为 **opt-in**。保证止于测试覆盖之处。

---

## 动机

Bitcask 用追加写数据日志 + 内存索引指向每个 key 的最新 live 记录。结构足够简单，适合做存储系统研究：可以一次只加一个机制——group commit、崩溃 oracle、带预算的 merge、value log——并在没有 LSM compaction 噪声主导的情况下观察代价。

本仓库的问题是：在 Go 里把这条基线推多远，同时对语义保持诚实。每条轨道在选项后实现机制、陈述不变量、做测试（相关处含真实进程崩溃）、做测量，并写明**没有**声称什么。

---

## 研究问题与发现

测量环境：Apple M3 Max · darwin/arm64 · Go 1.26.2 · `-benchmem -count=5`。  
含 sync / 子进程的绝对 `ns/op` **依赖设备**；跨机器优先看 `allocs/op` / `B/op`。原始输出：[`results/`](results/)。

| RQ | 问题 | 发现 |
| --- | --- | --- |
| 1 | 写延迟从哪来？ | 无 sync ≈ **39 µs**；每提交 sync ≈ **4.4 ms**（fsync 主导） |
| 2 | group commit 帮助有多大？ | 16 writer 下相对 every-commit 约 **14×**（636 µs vs 8.87 ms；syncs/commit 1.0→0.06） |
| 3 | 崩溃结果能否被检验而非口头保证？ | 真实 `os.Exit` + reopen + oracle；group-epoch 恢复 ≈ **37 ms/op** |
| 4 | merge 能否尊重延迟预算？ | budgeted ≈ **44 µs**（接近 no-merge）；static ≈ **1.74 ms** + ~69 KB rewrite |
| 5 | value 分离何时划算？ | 64 KB put 分离后约 **2×**（1.88 ms→914 µs）；get 仍约 39 µs |
| 6 | 受护栏的自治代价多少？ | 观测约 **+13 µs**/commit；apply 约 **33 ns**；永不削弱 sync 策略 |

完整论述：[`docs/paper.zh.md`](docs/paper.zh.md)。

---

## 六条轨道

| 轨 | 主题 | 章节 |
| --- | --- | --- |
| A | 写路径与分配 | [write-path](docs/tracks/write-path.md) |
| B | 耐久流水线 | [durability-pipeline](docs/tracks/durability-pipeline.md) |
| C | 崩溃一致性探索 | [crash-explorer](docs/tracks/crash-explorer.md) |
| D | SLO 感知 compaction | [slo-compaction](docs/tracks/slo-compaction.md) |
| E | 生命周期 KV 分离 | [lifecycle-kv-separation](docs/tracks/lifecycle-kv-separation.md) |
| F | 自治存储 | [autonomous-storage](docs/tracks/autonomous-storage.md) |

纯策略模型在 [`experiments/`](experiments/)，不依赖引擎包。

---

## 阅读顺序

1. [`docs/paper.zh.md`](docs/paper.zh.md) — 摘要、问题、结果、结论  
2. [`docs/tracks/`](docs/tracks/) 中感兴趣的机制章节  
3. [`ARCHITECTURE.zh.md`](ARCHITECTURE.zh.md) — 子系统 ↔ 文件  
4. [`results/`](results/) — 压测证据  

历史规划（v1.0 已关闭）：[`docs/ledger/research-roadmap.md`](docs/ledger/research-roadmap.md)。

---

## 复现

```bash
make check      # gofmt + go vet + go test + go test -race + git diff --check
make bench      # 六套 suite → results/track-*.txt  (COUNT=5)
make bench-a    # 单轨 (a…f)
```

说明：[`results/README.md`](results/README.md) ·
[`docs/methodology/benchmarks.md`](docs/methodology/benchmarks.md)。

---

## 仓库布局

```text
github.com/BreezeTeam/bitcask/
├── engine/           package bitcask — 对外引擎 API + 测试
├── internal/         rwmanager, helper, id
├── experiments/      纯模型（compaction / fault / autonomous / …）
├── docs/             论文、分轨、方法、台账
├── results/          提交的原始 bench 输出
├── example/          场景 workload
├── Makefile
├── README.md         English
└── README.zh.md      中文（本文）
```

```go
import "github.com/BreezeTeam/bitcask/engine"

db, err := bitcask.Open(bitcask.DefaultOptions)
```

---

## 局限（摘要）

- 单机合成微基准，不是多租户标定研究  
- Merge / value-log GC 为分阶段触发，不是 v1.0 连续后台 worker  
- 崩溃覆盖为语义故障点 + 进程突变退出，不是完整掉电仿真  
- 自治 apply 以观测为主且默认关闭；不声称闭环 QoS 收益  

详见各轨 *Limitations* 与论文 *Threats to validity*。

---

## 参考文献

- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [rosedb](https://github.com/flower-corp/rosedb) · [nutsdb](https://github.com/xujiajun/nutsdb)
- [从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
- [优雅的 Bitcask/BeansDB](https://zhuanlan.zhihu.com/p/53682577)
