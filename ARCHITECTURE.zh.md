# 架构

Bitcask 研究引擎的源码树地图。事务核心在 [`engine/`](engine/)，为 `package bitcask`（紧密耦合的 `*DB` 状态放在一个包——与 bbolt/badger 类似）。`internal/` 与 `experiments/` 分别承载 I/O 辅助与纯策略模型。

模块：`github.com/BreezeTeam/bitcask`  
对外导入：`github.com/BreezeTeam/bitcask/engine`（包名仍为 `bitcask`）

另见 [`docs/paper.zh.md`](docs/paper.zh.md)、[`docs/ledger/research-tracks-2026.zh.md`](docs/ledger/research-tracks-2026.zh.md)。  
English：[`ARCHITECTURE.md`](ARCHITECTURE.md)

**代码阅读路径：** `engine/tx.go`（提交可见性）→ `engine/group_commit.go`（fsync epoch）→
`engine/subprocess_crash_test.go`（突变退出 + reopen）→ `engine/value_log_gc.go`（指针安全）→
[`docs/tracks/`](docs/tracks/) 对应章节。

## 包布局

```
github.com/BreezeTeam/bitcask/
├── engine/                 package bitcask — 对外引擎 API + 测试/基准
├── internal/               引擎私有叶子包
│   ├── rwmanager/          文件读写抽象
│   ├── helper/             路径与 strconv
│   └── id/                 snowflake 事务 ID
├── experiments/            无依赖纯策略模型
├── docs/                   论文、分轨、方法、台账
├── results/                提交的原始基准输出
├── example/                场景 workload
├── Makefile
├── README.md / README.zh.md
└── ARCHITECTURE.md / ARCHITECTURE.zh.md
```

### 为何引擎仍是单 package

`db.go`、`tx.go`、`sync_policy.go` 等共享未导出的 `*DB` 字段。拆成多个可导入包会迫使导出内部状态或大改。结构通过 `engine/` 内聚包 + `internal/` 叶子 + `experiments/` 纯模型表达，而不是硬拆公共 API。

## 依赖方向

```
example ─┐
         ├─> engine (package bitcask) ─> internal/{rwmanager, helper, id}
tests ───┘         │
                   └─> experiments/*  （模型；experiments 不导入 engine）
```

## 从哪里读起

1. [`docs/paper.zh.md`](docs/paper.zh.md) — 问题、贡献、结果  
2. [`docs/ledger/research-tracks-2026.zh.md`](docs/ledger/research-tracks-2026.zh.md) — 完成矩阵  
3. [`docs/tracks/`](docs/tracks/) 感兴趣的分轨报告  
4. `engine/` 对应源码及其 `*_test.go`
