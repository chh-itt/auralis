# Auralis

**一个异步优先的 Rust 反应式内核：`Signal<T>` + `TaskScope`。**

*[English](README.md)*

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

三个 crate，零平台依赖，一个核心理念：**反应式 = 可暂停的异步任务；生命周期 = 所有权 + 结构化并发。**

---

## Crate 清单

| Crate | 职责 | 依赖 |
|---|---|---|
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, `memo!` 宏, batch 更新, 变更检测 future | **零** |
| `auralis-task` | `TaskScope`, 优先级执行器, `timer::sleep`, 取消, 上下文 DI, panic hook | `auralis-signal` |
| `auralis-devtools` | `ReactiveSnapshot`, `snapshot()`, `diff_snapshots()`, `ChangeStream`, `Timeline`, CLI | `auralis-signal`, `auralis-task` |

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

// ---- Signal::update（原地修改，零克隆）----
let items = Signal::new(vec![1, 2]);
items.update(|v| v.push(3));
assert_eq!(items.read(), vec![1, 2, 3]);

// ---- Signal::read_untracked（读取但不订阅）----
let config = Signal::new("dark_mode");
let _mode = config.read_untracked(); // 不会触发重新计算

// ---- TaskScope（结构化并发）----
let scope = TaskScope::new();
let c = count.clone();
let handle = scope.spawn(async move {
    loop {
        let val = c.changed().await;
        println!("count → {val}");
    }
});
handle.cancel(); // 取消单个任务，或 drop scope 取消全部
scope.on_cleanup(|| println!("scope 已丢弃"));

// ---- watch_effect（自动追踪副作用）----
scope.watch_effect(|| {
    println!("sum = {}", sum.read());
});
drop(scope); // 取消所有已 spawn 的任务 + 运行清理
```

## 为什么

传统反应式编程要求学习一套全新的运行时词汇——effect、cleanup、派生状态图、
调度器 tick。Auralis 将其还原为 Rust 开发者已经掌握的概念：

- **`await signal.changed()`**：任务挂起直到值发生变化
- **`TaskScope` 拥有任务**：drop scope 即取消其中的一切
- **事件/定时器/fetch 就是 future**：用 `select!`、`join!` 组合

不需要手动 cancel token，不需要"effect 系统"。纯异步 Rust。

## 设计取舍

Auralis 是一个响应式内核，不是框架。它刻意牺牲了三样东西：

- **无响应式图。** 每个 signal 持有一个平坦的订阅列表和单调版本号。没有拓扑传播，
  没有 Clean/Check/Dirty 状态机。代价：两个 effect 同读一个脏 Memo 可能各自
  触发一次重算。
- **无 Arena 分配。** 统一使用 `Rc<RefCell<>>`。没有 `Copy` 类型的 signal，
  没有 arena 生命周期。代价：每次读写有引用计数开销。
- **无多线程存储后端。** 设计为单线程（`!Send + !Sync`）。多线程 SSR 通过
  独立 executor 实例隔离。代价：不能直接跨线程共享 signal。

换来的是：三个 crate（信号 1,296 行，任务运行时 1,819 行，DevTools 663 行——纯 Rust 代码，不含注释/测试），信号层零依赖，
`#![forbid(unsafe_code)]`。整个响应式层一杯咖啡的时间就能读完。
如果你需要 debug 一个 effect 为什么不触发——你追踪的是一个平坦订阅列表，
而非一张图。

## 核心特性

**安全性**

- `#![forbid(unsafe_code)]` 所有 crate 零 unsafe，`#![warn(clippy::all, clippy::pedantic)]`
- Panic 安全的 Memo——compute panic 后旧订阅保留，下次 read 即可恢复
- Panic 安全的 batch——`BatchGuard` RAII 在 unwind 时恢复状态
- Panic 安全清理——`CallbackHandle::drop` 由 `catch_unwind` 隔离
- Memo 循环检测——thread-local 深度守卫
- 迭代式 scope 取消——BFS 叶到根，200+ 层级不爆栈

**性能**

- Signal crate 零依赖
- 单线程设计（`!Send` / `!Sync`）
- 时间预算可配置——`set_global_time_budget(ms)`
- 主动 waker 注销——防止僵尸 waker 堆积
- `Signal::update()`——原地修改，无需克隆

**诊断**

- `ReactiveSnapshot`——将全部 signal、memo 和依赖边导出为 JSON
- `diff_snapshots()`——精确看到两帧之间的变化
- `DerivationNode` 树——React DevTools 风格的数据流图
- `ChangeStream`——通过观察者钩子获取实时变更事件
- CLI——`auralis-devtools dump|stream|serve`
- `Signal`、`Memo`、`TaskScope` 支持可选标签
- Panic hook——`set_panic_hook()` 监听任务 panic
- 调度观察者——每次 signal 变更时触发的被动钩子

**易用性**

- `JoinHandle`——`spawn()` 返回可取消句柄
- `watch` / `watch_effect`——自动追踪副作用
- `Memo<T>`——惰性计算值，自动追踪依赖
- `SignalMap<T,U,F>`——轻量只读投影

## 多线程

`Signal<T>` 和 `TaskScope` 被设计为 `!Send + !Sync`——它们运行在
executor 线程上。跨线程通信通过标准通道桥接：工作线程持有 `Sender`
（`Send` 类型），宿主线程将 `Receiver` 的消息灌入 `sig.set()`。

```rust
use std::sync::mpsc;
use std::thread;

let sig = Signal::new(0i32);
let (tx, rx) = mpsc::channel();

thread::spawn(move || { tx.send(42).unwrap(); });

for msg in rx { sig.set(msg); }
assert_eq!(sig.read(), 42);
```

对于多请求 SSR 隔离（Tokio），每个请求使用独立的
`Executor::new_instance()` + `TaskScope`，通过 `with_executor` 包裹。
启用 `ssr-tokio` feature 即可获得 per-task scope 存储。

更多模式（多生产者、oneshot）见
`crates/auralis-task/examples/multi_thread_bridge.rs`。

## Tokio 集成

Auralis 不绑定运行时，但与 Tokio 天然互补。
**Tokio 负责 I/O**（网络、DB、定时器），将结果写入 signal。
**Auralis 负责响应式级联**——signal → memo → effect → 渲染输出。
边界仅是一行 `signal.set()`。

```rust
use auralis_signal::Signal;
use auralis_task::{Executor, TaskScope, with_executor};

// 每个请求：Tokio 取数据，Auralis 驱动渲染
let ex = Executor::new_instance();
let scope = TaskScope::with_executor(&ex);
let data = Signal::new(None::<Json>);

// 响应式效果：数据一变就重新渲染
scope.spawn(async move {
    loop { data.changed().await; render(data.read()); }
});

// Tokio I/O → signal → 响应式级联 →
// Executor::flush_instance(&ex) → 输出就绪
let json = reqwest::get(url).await?.json().await?;
with_executor(&ex, || data.set(Some(json)));
Executor::flush_instance(&ex);

// Drop scope → 所有响应式状态自动清理，无需手动 token
drop(scope);
```

需要 per-task scope 存储时（例如 `current_scope()` 在 tokio task 中
可用），启用 `ssr-tokio` feature 并在启动时调用
`init_scope_store_tokio()`。

完整可运行示例见 `demos/tokio-ssr/`。

## 目录结构

```
crates/
  auralis-signal/        # Signal<T>, Memo<T>, SignalMap<T,U,F>, batch(), futures
  auralis-task/          # TaskScope 树、执行器、timer、上下文 DI
  auralis-devtools/      # JSON 快照、diff、变更流、CLI
demos/
  egui-demo/             # Auralis vs 纯 egui 对比演示
  wasm-counter/          # Wasm 反应式计数器
  cli-multitask/         # CLI 多任务 Ctrl+C 取消演示
  leptos-devtools-demo/  # Leptos Todo Dashboard + Auralis DevTools
docs/
  vision-and-design.md   # 设计理念
  architecture.md        # 架构与模块设计
```

Xilem 集成层（`xilem_core_auralis`）见
[chh-itt/xilem](https://github.com/chh-itt/xilem) `auralis-experiment` 分支。

## Feature Flags

| Feature | 所属 Crate | 启用内容 |
|---------|-----------|---------|
| `debug` | `auralis-task` | `dump_reactive_graph()` — signal、memo 和 task 统一快照；同时启用 `auralis-signal/diagnostics` |
| `diagnostics` | `auralis-signal` | 响应式节点注册表、`ReactiveNodeSnapshot`、`dump_registry()` |
| `ssr-tokio` | `auralis-task` | Tokio task-local 存储，多请求 SSR 隔离 |
| `ws-transport` | `auralis-devtools` | WebSocket 服务（`serve` 命令），需要 `tungstenite` |

## 运行

```bash
# 全部测试
cargo test --all

# 指定 crate
cargo test -p auralis-signal
cargo test -p auralis-task
cargo test -p auralis-devtools

# Lint
cargo clippy --all-targets --all-features

# 性能基准
cargo run --example signal_bench --release -p auralis-signal
cargo run --example scope_bench --release -p auralis-task

# 示例
cargo run --example counter
cargo run --example multi_thread_bridge -p auralis-task
cargo run --example multi_instance_isolated -p auralis-task

# DevTools CLI
cargo run -p auralis-devtools -- dump

# 文档
cargo doc --open
```

## 最低 Rust 版本

Rust 1.80+

## 许可证

任选其一：

- MIT License ([LICENSE](LICENSE) 或 http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
