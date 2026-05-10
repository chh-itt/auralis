# Auralis

**一个异步优先的 Rust 反应式内核：`Signal<T>` + `TaskScope`。**

*[English](README.md)*

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

两个 crate，零平台依赖，一个核心理念：**反应式 = 可暂停的异步任务；生命周期 = 所有权 + 结构化并发。**

---

## Crate 清单

| Crate | 职责 | 依赖 |
|---|---|---|
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, `memo!` 宏, batch 更新, 变更检测 future | **零** |
| `auralis-task` | `TaskScope`, 优先级执行器, `timer::sleep`, 取消, 上下文 DI, panic hook | 仅 `auralis-signal` |

## 快速开始

```rust
use auralis_signal::{Signal, Memo, batch};
use auralis_task::{TaskScope, set_global_time_budget};

// ---- Signal ----
let count = Signal::new(0);
count.set(1);
assert_eq!(count.read(), 1);

// ---- Memo（自动追踪计算值）----
let a = Signal::new(2);
let b = Signal::new(3);
let sum = Memo::new(move || a.read() + b.read());
assert_eq!(sum.read(), 5);

// ---- SignalMap（轻量只读投影）----
let names = Signal::new(vec!["alice", "bob"]);
let len = names.map(|v: &Vec<&str>| v.len());
assert_eq!(len.read(), 2);

// ---- Batch（多次 set，一次通知）----
let x = Signal::new(0);
batch(|| {
    x.set(1);
    x.set(2);
    x.set(3);
});
assert_eq!(x.read(), 3);

// ---- TaskScope（结构化并发）----
let scope = TaskScope::new();
let c = count.clone();
scope.spawn(async move {
    loop {
        let val = c.changed().await;
        println!("count → {val}");
    }
});
drop(scope); // 取消所有已 spawn 的任务
```

## 为什么

传统反应式编程要求学习一套全新的运行时词汇——effect、cleanup、派生状态图、
调度器 tick。Auralis 将其还原为 Rust 开发者已经掌握的概念：

- **`await signal.changed()`**：任务挂起直到值发生变化
- **`TaskScope` 拥有任务**：drop scope 即取消其中的一切
- **事件/定时器/fetch 就是 future**：用 `select!`、`join!` 组合

不需要 `on_cleanup` 钩子，不需要手动 cancel token，不需要"effect 系统"。
纯异步 Rust。

## 核心特性

- **`#![forbid(unsafe_code)]`**——两个 crate 均零 unsafe
- **`#![warn(clippy::all, clippy::pedantic)]`**——严格 lint
- **Signal crate 零依赖**
- **单线程设计**（`!Send` / `!Sync`）；多线程场景使用 `Executor::new_instance()` 实例隔离
- **时间预算可配置**——`set_global_time_budget(ms)` 适配不同帧率
- **Panic hook**——`set_panic_hook(hook)` 监听任务 panic
- **Panic 安全的 batch**——`BatchGuard` RAII 在 unwind 时恢复状态
- **Panic 安全的 Memo**——compute panic 后旧订阅保留，下次 read 即可恢复
- **主动 waker 注销**——防止僵尸 waker 堆积
- **迭代式 scope 取消**——BFS 叶到根，200+ 层级不爆栈

## 目录结构

```
crates/
  auralis-signal/       # Signal<T>, Memo<T>, SignalMap<T,U,F>, batch()
    src/
      signal.rs         # Signal 状态机、订阅者管理
      memo.rs           # Memo 惰性追踪、panic 安全重算
      batch.rs          # BatchGuard、batch()、in_batch()
      observer.rs       # ObserverState、OBSERVER thread-local
      future.rs         # SignalChangedFuture, MapChangedFuture, FilterChangedFuture
  auralis-task/         # TaskScope 树、执行器、timer、上下文 DI
    src/
      executor.rs       # 优先级执行器、时间预算、延迟回调
      scope.rs          # TaskScope、CallbackHandle、上下文系统
      timer.rs          # timer::sleep() 协作延迟
        debug.rs          # dump_task_tree()（feature-gated）
    examples/
      counter.rs        # 可运行的 CLI 示例
    tests/
      signal_task_integration.rs  # 跨 crate 集成测试
demos/
  egui-demo/            # Auralis vs 纯 egui 对比演示
    examples/
      perf_report.rs    # 无头性能基准
  wasm-counter/         # Wasm 反应式计数器 (Signal + Memo + timer)
  cli-multitask/        # CLI 多任务 Ctrl+C 取消演示
docs/
  vision-and-design.md  # 设计理念（英文）
  architecture.md       # 架构与模块（英文）
  愿景与设计理念.md       # 设计理念（中文）
  架构与模块设计.md       # 架构与模块（中文）
```

## Feature Flags

| Feature | 所属 Crate | 启用内容 |
|---------|-----------|---------|
| `debug` | `auralis-task` | `dump_task_tree()` 诊断 |
| `ssr-tokio` | `auralis-task` | Tokio task-local 存储，多请求 SSR 隔离 |

## 运行

```bash
# 全部测试
cargo test --all

# 指定 crate
cargo test -p auralis-signal
cargo test -p auralis-task

# Lint
cargo clippy --all-targets -- -D warnings

# 性能基准（非 Wasm 环境）
cargo bench -p auralis-signal
cargo bench -p auralis-task

# 示例
cargo run --example counter

# 多线程桥接示例
cargo run --example multi_thread_bridge -p auralis-task

# 文档
cargo doc --open
```

## 最低 Rust 版本

Rust 1.80+

## 许可证

任选其一：

- MIT License ([LICENSE](LICENSE) 或 http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
