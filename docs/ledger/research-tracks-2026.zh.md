# 六轨状态（v1.0.0）

Bitcask 研究计划的完成记录。**已完成。**  
叙述与发现：[`../paper.zh.md`](../paper.zh.md)。分轨：[`../tracks/`](../tracks/)。  
English：[`research-tracks-2026.md`](research-tracks-2026.md)

一轨完成标准：机制、不变量、测试、基准、实测报告、局限、可复现命令。六轨均已满足。

## 矩阵

| 轨 | 研究问题 | 发现 | 报告 |
| --- | --- | --- | --- |
| A — 写路径 | 哪些写成本固定 / 比例 / 可避免？ | 小 put ≈39 µs；每提交 sync ≈4.4 ms；encode 复用 → 0 allocs/op | [write-path](../tracks/write-path.md) |
| B — 耐久 | writer 能否共享 fsync 屏障且无虚假耐久？ | 16 writer 下 group commit 约 14×（syncs/commit 1.0→0.06） | [durability-pipeline](../tracks/durability-pipeline.md) |
| C — 崩溃一致性 | 崩溃结果能否对 oracle 回放？ | 真 `os.Exit` + reopen；group-epoch 恢复 ≈37 ms/op | [crash-explorer](../tracks/crash-explorer.md) |
| D — compaction | 测得的垃圾/热度能否驱动延迟预算？ | budgeted ≈44 µs vs static ≈1.74 ms | [slo-compaction](../tracks/slo-compaction.md) |
| E — KV 分离 | 分离何时划算，GC 如何安全？ | 64 KB put 约 2×；get ≈39 µs；指针安全 GC | [lifecycle-kv-separation](../tracks/lifecycle-kv-separation.md) |
| F — 自治 | 相位检测推荐策略且不削弱 sync？ | 观测 ≈+13 µs/commit；apply ≈33 ns；不削弱 sync | [autonomous-storage](../tracks/autonomous-storage.md) |

证据：[`../../results/`](../../results/)。命令：`make check` · `make bench` / `make bench-a`…`f`。

## 完成条（A–F）

- [x] 机制在 opt-in 选项之后（默认 Bitcask 路径保留）
- [x] 陈述正确性不变量
- [x] 确定性测试（需要处含故障/子进程）
- [x] 基准与分轨报告中的实测表
- [x] 局限与可复现命令已记录

## 跨轨不变量

1. v1.0 内默认 `github.com/BreezeTeam/bitcask/engine` API 与盘格式保持向后兼容。  
2. 性能与崩溃声明引用命令，并在适用时引用 `results/` 文件。  
3. 纯策略模型在 `experiments/`；持久化与恢复在引擎内。  
4. 自治 apply 默认关闭，且不得削弱显式 sync 策略。  
5. Merge / value-log GC 经分阶段、fsync 的 manifest 发布后再丢弃源。  
6. 禁止对索引/waiter/恢复可能持有的对象做 unsafe 池化。  
7. 连续后台 compaction worker 不在 v1.0 范围。

## 计划沿革

里程碑 v0.3→v1.0 已关闭。原始路线意图（英文存档）：[`research-roadmap.md`](research-roadmap.md)。
