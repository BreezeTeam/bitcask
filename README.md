# bitcask

A Go Bitcask-style key/value storage engine for learning, benchmarking, and experimenting with modern KV storage design.

## 项目目标

这个项目最初用于学习 Bitcask/log-structured hash table。当前目标是把它演进成一个 KV storage learning lab：既保留简单可读的 Bitcask 主线，又逐步加入 crash recovery、compaction、range scan、mini-LSM、KV separation、sync policy、fault injection、observability 等实验能力。

## 快速运行

```bash
go test -count=1 ./...
go test . -run '^$' -bench 'BenchmarkWritePath|BenchmarkTxCommit|BenchmarkSmallValue' -benchmem
go test ./example -run '^$' -bench 'BenchmarkBackend' -benchmem
```

## 当前学习模块

- Core Bitcask: append-only data files, transaction commit marker, B+ tree index, TTL, delete tombstone, merge, range/prefix scan.
- Example backend: ecommerce-like inventory, order, session, and mixed-content backend storage workloads.
- Experiments: workload generator, adaptive sync model, compaction picker, KV separation model, fault schedule model, mini-LSM, and Raft log model.
- Benchmarks: write-path microbenchmarks, end-to-end transaction baselines, small-value fixed-cost study, sync policy comparison, compaction/KV-separation/fault experiments, and backend scenario benchmarks.

## Research docs

- [Research roadmap](docs/research-roadmap.md)
- [Benchmark methodology](docs/benchmarks.md)
- [Write path](docs/write-path.md)
- [Storage format](docs/storage-format.md)
- [Recovery and compaction](docs/recovery-and-compaction.md)
- [Experiments](docs/experiments.md)
- [Observability](docs/observability.md)
- [References](docs/references.md)

## References

- [rosedb](https://github.com/flower-corp/rosedb)
- [nutsdb](https://github.com/xujiajun/nutsdb)
- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [从零实现一个 k-v 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
- [优雅的 Bitcask/BeansDB](https://zhuanlan.zhihu.com/p/53682577)
