//! Integration tests covering signal + task cross-crate scenarios:
//! scope-bound signal watching, batch + task interaction, memo + task,
//! and cancellation mid-await.
//!
//! Each test resets the executor first so tests don't leak state.

use std::cell::Cell;
use std::rc::Rc;

use auralis_signal::{batch, Memo, Signal};
use auralis_task::{
    init_flush_scheduler, init_time_source, reset_executor_for_test, set_deferred, Priority,
    ScheduleFlush, TaskScope, TimeSource,
};

struct SyncScheduler;
impl ScheduleFlush for SyncScheduler {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

struct TestClock {
    now: Cell<u64>,
}
impl TestClock {
    fn new(initial: u64) -> Self {
        Self {
            now: Cell::new(initial),
        }
    }
}
impl TimeSource for TestClock {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
}

fn setup() -> Rc<TestClock> {
    reset_executor_for_test();
    let sched: Rc<dyn ScheduleFlush> = Rc::new(SyncScheduler);
    init_flush_scheduler(sched);
    let clock = Rc::new(TestClock::new(0));
    init_time_source(clock.clone());
    clock
}

#[test]
fn signal_change_wakes_spawned_task() {
    setup();

    let sig = Signal::new(0);
    let executed = Rc::new(Cell::new(false));

    let s = sig.clone();
    let e = Rc::clone(&executed);
    let scope = TaskScope::new();
    scope.spawn(async move {
        s.changed().await;
        e.set(true);
    });

    sig.set(42);
    assert!(executed.get());
}

#[test]
fn scope_drop_cancels_awaiting_task() {
    setup();

    let sig = Signal::new(0);
    let dropped = Rc::new(Cell::new(false));
    let d = Rc::clone(&dropped);

    struct Guard(Rc<Cell<bool>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    {
        let _scope = TaskScope::new();
        let s = sig.clone();
        _scope.spawn(async move {
            let _g = Guard(d);
            // This await never completes — scope is dropped first.
            s.changed().await;
            unreachable!();
        });
    }

    // The guard should have been dropped when the scope was dropped,
    // meaning the task's future was dropped.
    assert!(dropped.get());
}

#[test]
fn batch_multiple_sets_wakes_once() {
    setup();

    let sig = Signal::new(0);
    let count = Rc::new(Cell::new(0u32));
    let c = Rc::clone(&count);

    let scope = TaskScope::new();
    let s = sig.clone();
    scope.spawn(async move {
        loop {
            s.changed().await;
            c.set(c.get() + 1);
        }
    });

    batch(|| {
        sig.set(1);
        sig.set(2);
        sig.set(3);
    });

    assert_eq!(count.get(), 1);
}

#[test]
fn memo_change_wakes_task() {
    setup();

    let a = Signal::new(2);
    let b = Signal::new(3);
    let a2 = a.clone();
    let b2 = b.clone();
    let sum = Memo::new(move || a2.read() + b2.read());

    let executed = Rc::new(Cell::new(false));
    let e = Rc::clone(&executed);
    let scope = TaskScope::new();
    let s = sum.clone();
    scope.spawn(async move {
        s.changed().await;
        e.set(true);
    });

    a.set(10);
    assert!(executed.get());
}

#[test]
fn set_deferred_does_not_panic_in_task_drop() {
    setup();

    let sig = Signal::new(0);

    {
        let scope = TaskScope::new();
        let s = sig.clone();
        scope.spawn(async move {
            // This task is cancelled before completing.
            s.changed().await;
        });

        // set_deferred is safe even when called during scope drop.
        set_deferred(&sig, 42);
        // Scope drops here, cancelling the task.
    }

    // flush should have processed the deferred set.
    assert_eq!(sig.read(), 42);
}

#[test]
fn high_priority_task_spawns_and_runs() {
    setup();

    let executed = Rc::new(Cell::new(false));
    let e = Rc::clone(&executed);

    let scope = TaskScope::new();
    scope.spawn_with_priority(Priority::High, async move {
        e.set(true);
    });

    // With sync scheduler, task runs immediately.
    assert!(executed.get());
}

#[test]
fn deep_scope_tree_no_leak_on_batch_cancel() {
    setup();

    let sig = Signal::new(0);
    let alive_count = Rc::new(Cell::new(0u32));

    {
        let root = TaskScope::new();
        let mut current = root.clone();
        // Build a deep chain.
        for _ in 0..50 {
            let child = TaskScope::new_child(&current);
            let s = sig.clone();
            let a = Rc::clone(&alive_count);
            a.set(a.get() + 1);
            child.spawn(async move {
                let _guard = IncrementOnDrop(Rc::clone(&a));
                s.changed().await;
            });
            current = child;
        }
    }
    // All tasks dropped → alive_count back to 0.
    assert_eq!(alive_count.get(), 0);
}

struct IncrementOnDrop(Rc<Cell<u32>>);
impl Drop for IncrementOnDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}
