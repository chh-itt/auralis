/// Criterion benchmarks for auralis-task.
///
/// Run with: `cargo bench -p auralis-task`
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::cell::RefCell;
use std::rc::Rc;

use auralis_task::{init_flush_scheduler, ScheduleFlush, TaskScope};

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
// Batched scheduler — collects callbacks, flushes on demand.
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
// benchmark 1: TaskScope create + destroy (100 tasks)
// ---------------------------------------------------------------------------

fn bench_scope_create_destroy(c: &mut Criterion) {
    c.bench_function("scope_100_tasks_create_destroy", |b| {
        init();
        b.iter(|| {
            let scope = TaskScope::new();
            for _ in 0..100 {
                scope.spawn(async {});
            }
            let _ = black_box(scope);
        });
    });
}

// ---------------------------------------------------------------------------
// benchmark 2: deep nesting drop (200 levels)
// ---------------------------------------------------------------------------

fn bench_deep_nesting_drop(c: &mut Criterion) {
    c.bench_function("scope_200_levels_deep_drop", |b| {
        init();
        b.iter(|| {
            let root = TaskScope::new();
            {
                let mut current = TaskScope::new_child(&root);
                for _ in 0..199 {
                    current.spawn(async {});
                    current = TaskScope::new_child(&current);
                }
            }
            let _ = black_box(&root);
        });
    });
}

// ---------------------------------------------------------------------------
// benchmark 3: priority ordering (batched flush)
// ---------------------------------------------------------------------------

fn bench_priority_ordering(c: &mut Criterion) {
    use auralis_task::Priority;
    use std::cell::RefCell;

    c.bench_function("priority_1000_low_10_high", |b| {
        let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));

        b.iter(|| {
            // Use a batched scheduler so tasks accumulate in the queues
            // rather than running synchronously during each spawn.
            let sched = BatchedFlush::new();
            init_flush_scheduler(Rc::clone(&sched) as Rc<dyn ScheduleFlush>);
            order.borrow_mut().clear();

            // Spawn 1000 low-priority tasks.
            for i in 0..1000 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::Low, async move {
                    o.borrow_mut().push(format!("low_{}", i));
                });
            }

            // Spawn 10 high-priority tasks.
            for i in 0..10 {
                let o = Rc::clone(&order);
                auralis_task::spawn_global_with_priority(Priority::High, async move {
                    o.borrow_mut().push(format!("high_{}", i));
                });
            }

            // Now flush — high-priority tasks run first.
            sched.drain();

            let result = order.borrow().clone();
            // Verify high-priority tasks ran first.
            for (i, item) in result.iter().enumerate().take(10) {
                assert!(
                    item.starts_with("high_"),
                    "expected high-priority task at index {i}, got: {item}"
                );
            }
            // Total count should be correct.
            assert_eq!(result.len(), 1010);
        });

        // Restore sync scheduler for other benchmarks.
        init();
    });
}

criterion_group!(
    benches,
    bench_scope_create_destroy,
    bench_deep_nesting_drop,
    bench_priority_ordering,
);
criterion_main!(benches);
