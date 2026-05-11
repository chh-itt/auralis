/// Scope performance benchmarks.
///
/// Run with: `cargo run --example scope_bench --release`
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use auralis_task::{init_flush_scheduler, Priority, ScheduleFlush, TaskScope};

// ---------------------------------------------------------------------------
// Sync scheduler for deterministic benchmarks
// ---------------------------------------------------------------------------

struct BenchScheduleFlush;
impl ScheduleFlush for BenchScheduleFlush {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}
fn init() {
    init_flush_scheduler(Rc::new(BenchScheduleFlush));
}

// ---------------------------------------------------------------------------
// Batched scheduler
// ---------------------------------------------------------------------------

struct BatchedFlush {
    callbacks: RefCell<Vec<Box<dyn FnOnce()>>>,
}
impl BatchedFlush {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            callbacks: RefCell::new(Vec::new()),
        })
    }
    fn drain(&self) {
        while let Some(cb) = self.callbacks.borrow_mut().pop() {
            cb();
        }
    }
}
impl ScheduleFlush for BatchedFlush {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        self.callbacks.borrow_mut().push(callback);
    }
}

// ---------------------------------------------------------------------------
// Timer helper
// ---------------------------------------------------------------------------

fn time<R>(label: &str, iterations: u64, mut f: impl FnMut() -> R) {
    // Warm-up
    for _ in 0..(iterations / 10).max(1) {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0 / iterations as f64;
    println!("  {:<42} {:>8.2} µs/iter", label, elapsed);
}

// ---------------------------------------------------------------------------

fn main() {
    println!("=== Auralis Task Scope Benchmarks ===\n");

    // 1. Scope create + destroy (100 tasks)
    let iterations = 50_000;
    init();
    time("scope_100_tasks_create_destroy", iterations, || {
        let scope = TaskScope::new();
        for _ in 0..100 {
            scope.spawn(async {});
        }
        drop(scope);
    });

    // 2. Deep nesting drop (200 levels)
    init();
    time("scope_200_levels_deep_drop", 5_000, || {
        let root = TaskScope::new();
        {
            let mut current = TaskScope::new_child(&root);
            for _ in 0..199 {
                current.spawn(async {});
                current = TaskScope::new_child(&current);
            }
        }
        drop(root);
    });

    // 3. Priority ordering (batched flush)
    {
        let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let min_iterations = 5_000;
        // Warm-up
        for _ in 0..500 {
            let sched = BatchedFlush::new();
            init_flush_scheduler(Rc::clone(&sched) as Rc<dyn ScheduleFlush>);
            order.borrow_mut().clear();
            for i in 0..1000 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::Low, async move {
                    o.borrow_mut().push(format!("low_{i}"));
                });
            }
            for i in 0..10 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::High, async move {
                    o.borrow_mut().push(format!("high_{i}"));
                });
            }
            sched.drain();
        }

        let start = Instant::now();
        for _ in 0..min_iterations {
            let sched = BatchedFlush::new();
            init_flush_scheduler(Rc::clone(&sched) as Rc<dyn ScheduleFlush>);
            order.borrow_mut().clear();
            for i in 0..1000 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::Low, async move {
                    o.borrow_mut().push(format!("low_{i}"));
                });
            }
            for i in 0..10 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::High, async move {
                    o.borrow_mut().push(format!("high_{i}"));
                });
            }
            sched.drain();
            // Verify correctness.
            let result = order.borrow().clone();
            debug_assert_eq!(result.len(), 1010);
        }
        let elapsed = start.elapsed().as_secs_f64() * 1_000_000.0 / min_iterations as f64;
        println!(
            "  {:<42} {:>8.2} µs/iter",
            "priority_1000_low_10_high", elapsed
        );
        init(); // restore sync scheduler
    }

    // 4. Scope churn (100 scopes x 10 tasks, batch drop)
    init();
    time("scope_churn_100x10_batch_drop", 5_000, || {
        let scopes: Vec<TaskScope> = (0..100)
            .map(|_| {
                let s = TaskScope::new();
                for _ in 0..10 {
                    s.spawn(async {});
                }
                s
            })
            .collect();
        drop(scopes);
    });

    // 5. Scope suspend + resume (1000 tasks)
    init();
    let scope = TaskScope::new();
    for _ in 0..1000 {
        scope.spawn(async {});
    }
    time("scope_suspend_resume_1000_tasks", 500_000, || {
        scope.suspend();
        scope.resume();
    });

    // 6. Wide shallow tree drop (50x50, ~2550 tasks)
    init();
    time("scope_wide_tree_50x3_drop", 500, || {
        let root = TaskScope::new();
        for _ in 0..50 {
            let child = TaskScope::new_child(&root);
            child.spawn(async {});
            for _ in 0..50 {
                let grandchild = TaskScope::new_child(&child);
                grandchild.spawn(async {});
            }
        }
        drop(root);
    });

    println!("\n=== Done ===");
}
