# Auralis

**一个 Rust 反应式内核 —— `Signal<T>` + `TaskScope`。**

*[English](README.md)*

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)

两个核心 crate，零平台依赖。`Signal<T>` 和 `Memo<T>` 在运行时追踪依赖。
`TaskScope` 通过所有权管理任务生命周期。

---

## 快速开始

```rust
use auralis_signal::{Signal, Memo, batch};
use auralis_task::TaskScope;

// ---- Signal ----
let count = Signal::new(0);
count.set(1);
assert_eq!(count.read(), 1);

// ---- Memo（自动追踪计算值）----
let a = Signal::new(2);
let b = Signal::new(3);
let sum = Memo::new(move || a.read() + b.read());
assert_eq!(sum.read(), 5);

// ---- Signal::update（原地修改）----
let items = Signal::new(vec![1, 2]);
items.update(|v| v.push(3));

// ---- Batch（多次 set，一次通知）----
batch(|| { x.set(1); x.set(2); });

// ---- TaskScope（结构化并发）----
let scope = TaskScope::new();
let c = count.clone();
scope.spawn(async move {
    loop { c.changed().await; println!("count → {}", c.read()); }
});
drop(scope); // 取消内部全部任务
```

---

## 它是什么

Auralis 是一个反应式内核，不是框架。它有三个刻意的取舍，把每个 crate 控制在
~3,000 行以内：

**无反应式图。** 每个 signal 持有一个平坦的订阅列表和单调版本号。没有拓扑传播，
没有 Clean/Check/Dirty 状态机。代价：两个 effect 同读一个脏 Memo 可能各自触发
一次重算。

**无 Arena 分配。** 统一使用 `Rc<RefCell<>>`。没有 `Copy` 类型的 signal，没有
arena 生命周期。代价：每次读写有引用计数开销。

**无多线程存储后端。** 设计为单线程（`!Send + !Sync`）。多线程场景通过独立
executor 实例隔离，通道桥接。代价：你需要自己管理隔离边界。

换来的是：`#![forbid(unsafe_code)]` 全覆盖，`auralis-signal` 零依赖，一个下午
就能读完的代码量。不需要 effect 系统，不需要调度器词汇表。`Signal::read()` 自动
订阅，`Signal::set()` 自动通知。任务通过 `TaskScope` 拥有自身生命周期。

---

## Crate 清单

| Crate | 职责 | 规模 | 依赖 |
|---|---|---|---|
| `auralis-signal` | `Signal<T>`, `Memo<T>`, `SignalMap`, batch 更新, 变更检测 future | ~2,700 行 | **零** |
| `auralis-task` | `TaskScope`, 优先级执行器, `timer::sleep`, 取消, 上下文 DI | ~3,000 行 | `auralis-signal` |
| `auralis-devtools` | `ReactiveSnapshot`, `diff_snapshots()`, `ChangeStream`, CLI | ~1,100 行 | `auralis-signal`, `auralis-task` |

---

## 谁在用

**[Burin](https://github.com/chh-itt/burin)** —— 一个保留模式的 Rust GUI 框架，
60 个内置 Widget，GPU/CPU 双渲染后端，Taffy 增量布局，无窗口全帧测试。
Auralis 驱动了 Burin 的整条反应式管线：`Signal::set()` → 脏标记 →
增量布局 → 子树缓存 → 绘制 → GPU。

---

## 核心特性

**安全性**
- `#![forbid(unsafe_code)]` 全部 crate
- Panic 安全的 Memo —— compute panic 后旧订阅保留，下次 read() 即可恢复
- Panic 安全的 batch —— `BatchGuard` RAII 在 unwind 时恢复状态
- Panic 安全的清理 —— `CallbackHandle::drop` 由 `catch_unwind` 隔离
- Memo 循环检测 —— thread-local 深度守卫
- 迭代式 scope 取消（BFS 叶到根），200+ 层级不爆栈

**性能**
- Signal crate 零依赖；单线程设计（`!Send` / `!Sync`）
- 时间预算可配置 —— `set_global_time_budget(ms)`
- 主动 waker 注销 —— 防止僵尸 waker 堆积
- `Signal::update()` —— 原地修改，无需克隆

**诊断**
- `ReactiveSnapshot` —— 将全部 signal、memo 和依赖边导出为 JSON
- `diff_snapshots()` —— 精确查看两帧之间的变化
- `DerivationNode` 树 —— React DevTools 风格的数据流图
- `ChangeStream` —— 通过观察者钩子获取实时变更事件
- CLI —— `auralis-devtools dump | stream | serve`
- `Signal`、`Memo`、`TaskScope` 支持可选标签
- `set_panic_hook()` 监听任务 panic
- 调度观察者 —— 每次 signal 变更时触发的被动钩子

**易用性**
- `spawn()` 返回 `JoinHandle` —— 可取消、可检查单个任务
- `watch` / `watch_effect` —— 自动追踪副作用
- `Memo<T>` —— 惰性计算值，自动追踪依赖
- `SignalMap<T,U,F>` —— 轻量只读投影

---

## 多线程

`Signal<T>` 和 `TaskScope` 设计为 `!Send + !Sync` —— 它们运行在 executor 线程上。
跨线程通信通过标准通道桥接：

```rust
use std::sync::mpsc;
use std::thread;

let sig = Signal::new(0i32);
let (tx, rx) = mpsc::channel();

thread::spawn(move || { tx.send(42).unwrap(); });
for msg in rx { sig.set(msg); }
assert_eq!(sig.read(), 42);
```

多请求 SSR 隔离（Tokio）：每个请求使用独立的 `Executor::new_instance()` +
`TaskScope`。启用 `ssr-tokio` feature 即可获得 per-task scope 存储。
更多模式见 `crates/auralis-task/examples/multi_thread_bridge.rs`。

---

## Feature Flags

| Feature | 所属 Crate | 启用内容 |
|---------|-----------|---------|
| `debug` | `auralis-task` | `dump_reactive_graph()` —— signal、memo 和 task 统一快照 |
| `diagnostics` | `auralis-signal` | 响应式节点注册表、`ReactiveNodeSnapshot`、`dump_registry()` |
| `ssr-tokio` | `auralis-task` | Tokio task-local 存储，多请求 SSR 隔离 |
| `ws-transport` | `auralis-devtools` | WebSocket 服务（`serve` 命令），需要 `tungstenite` |

---

## 运行

```bash
cargo test --all
cargo test -p auralis-signal
cargo test -p auralis-task
cargo test -p auralis-devtools

cargo clippy --all-targets --all-features

cargo run --example signal_bench --release -p auralis-signal
cargo run --example scope_bench --release -p auralis-task

cargo run -p auralis-devtools -- dump
cargo doc --open
```

---

## 最低 Rust 版本

Rust 1.80+

---

## 许可

任选其一：

- MIT License ([LICENSE](LICENSE) 或 http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
