//! Counter demo: signal change → async task reaction → scope cleanup.
//!
//! Run with: `cargo run --example counter`

use std::cell::Cell;
use std::rc::Rc;

use auralis_signal::{batch, Memo, Signal};
use auralis_task::{init_flush_scheduler, init_time_source, ScheduleFlush, TaskScope, TimeSource};

/// A synchronous scheduler — callbacks fire immediately.
/// Good enough for CLI demos.  In a browser you'd use
/// `requestAnimationFrame` or `queueMicrotask`.
struct CliScheduler;
impl ScheduleFlush for CliScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

struct CliClock {
    start: std::time::Instant,
}
impl CliClock {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}
impl TimeSource for CliClock {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

fn main() {
    // One-time executor setup.
    init_flush_scheduler(Rc::new(CliScheduler));
    init_time_source(Rc::new(CliClock::new()));

    // ---- Signal basics ----
    println!("=== Signal basics ===");
    let count = Signal::new(0);
    println!("count = {}", count.read());
    count.set(42);
    println!("count = {}", count.read());

    // ---- Memo (auto-tracking computed value) ----
    println!("\n=== Memo ===");
    let a = Signal::new(2);
    let b = Signal::new(3);
    let a2 = a.clone();
    let b2 = b.clone();
    let sum = Memo::new(move || a2.read() + b2.read());
    println!("2 + 3 = {}", sum.read());
    a.set(10);
    println!("10 + 3 = {}", sum.read());

    // ---- TaskScope: spawn a task that watches a signal ----
    println!("\n=== TaskScope (scoped signal watcher) ===");
    let messages = Signal::new(vec!["hello".to_string()]);

    {
        let scope = TaskScope::new();
        let msgs = messages.clone();
        scope.spawn(async move {
            loop {
                let val = msgs.changed().await;
                println!("  task observed: {:?}", val);
            }
        });

        messages.set(vec!["hello".to_string(), "world".to_string()]);
        messages.set(vec![
            "hello".to_string(),
            "world".to_string(),
            "!".to_string(),
        ]);

        // Explicit drop — cancels the task and cleans up resources.
        drop(scope);
    }
    println!("(scope dropped — task and subscriptions cleaned up)");

    // ---- Batch: multiple sets, one notification ----
    println!("\n=== Batch ===");
    let x = Signal::new(0);
    let notifications = Rc::new(Cell::new(0u32));
    let n = Rc::clone(&notifications);
    let x2 = x.clone();

    let scope = TaskScope::new();
    scope.spawn(async move {
        loop {
            x2.changed().await;
            n.set(n.get() + 1);
        }
    });

    batch(|| {
        x.set(1);
        x.set(2);
        x.set(3);
    });
    println!(
        "After 3 sets in a batch: {} notification(s)",
        notifications.get()
    );
    // Should be 1 — not 3.

    println!("\nDone.");
}
