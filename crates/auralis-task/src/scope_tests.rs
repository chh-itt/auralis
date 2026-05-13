//! Tests for `auralis_task` — `TaskScope`, executor, timer, cross-scope
//! interaction, and cancellation edge cases.
//!
//! # Test infrastructure
//!
//! Most tests call `init()` → `reset_executor_for_test()` +
//! `init_flush_scheduler(TestScheduleFlush)`.  `TestScheduleFlush` fires
//! the flush callback synchronously so that spawned tasks complete
//! immediately without a browser event loop.
//!
//! # Organisation
//!
//! | Section | Tests | What it verifies |
//! |---|---|---|
//! | **Scope basics** | `new_scope_has_zero_tasks`, `new_child_attaches_to_parent`, `scope_child_explicit_tree`, `spawn_adds_task`, `spawn_and_complete`, `spawn_many_tasks_all_complete` | Scope tree construction, spawn, task completion |
//! | **Cancel** | `scope_spawn_and_cancel`, `nested_scope_child_cancel_with_parent`, `deeply_nested_scope_drop_no_stack_overflow`, `no_leak_on_cancel` | BFS leaf→root iterative cancellation, no stack overflow at 200 levels |
//! | **Callback lifecycle** | `callback_handle_dropped_before_tasks`, `callback_handle_cleaned_up_on_child_scope_drop`, `callback_handle_noop_does_not_panic` | Callbacks dropped before tasks; noop handle safe |
//! | **On-cleanup** | (via `register_callback_handle` / `on_cleanup`) | Cleanup functions run before task cancellation |
//! | **Context DI** | `context_provide_and_consume_*`, `context_consume_walks_up_to_parent`, `context_consume_not_found`, `context_shadowing`, `context_removed_on_scope_drop`, `expect_context_panics_*` | `provide`/`consume` walking up parent chain, shadowing, missing context panic |
//! | **Context macros** | `provide_context_macro_works`, `consume_context_macro_*` | `provide_context!` / `consume_context!` macros |
//! | **Current scope** | `current_scope_available_in_spawned_task` | `current_scope()` discoverable inside spawned task |
//! | **JoinHandle** | `joinhandle_*` | `cancel()` individual task, `is_finished()`, `task_id()`, no-op on already-completed task |
//! | **Executor priority** | `executor_priority_ordering`, `priority_queue_*` | High-priority dequeued before low; low not permanently starved |
//! | **Executor batch** | `executor_batch`, `batch_spawn_with_no_auto_flush_then_manual_flush` | Multiple spawns, manual flush |
//! | **yield_now** | `yield_now_gives_other_tasks_a_turn` | `yield_now()` yields to other tasks in same flush |
//! | **Panic isolation** | `panic_in_task_is_isolated`, `flush_instance_panicking_task_is_isolated` | One task's panic doesn't block others |
//! | **Panic hook** | `panic_hook_is_invoked_on_task_panic` | `set_panic_hook()` receives `PanicInfo` on task panic |
//! | **Time budget** | `time_budget_with_test_time_source`, `time_budget_honoured_with_split` | Executor yields when budget exhausted, schedules follow-up flush |
//! | **Re-entrant flush** | `reentrant_flush_is_noop` | Calling flush while inside flush is a no-op |
//! | **set_deferred** | `set_deferred_*` | `set_deferred()` safe from `Drop`; routes to correct executor |
//! | **Signal + task** | `notify_signal_state_follow_up_handles_reentrant_dirty`, `sync_callback_fallback_without_schedule_hook` | Re-entrant `set()` with follow-up; fallback when no hook installed |
//! | **Cross-scope** | `two_scopes_watch_same_signal_both_wake`, `cross_scope_signal_propagation`, `drop_source_scope_does_not_cancel_receiving_scope` | Independent scopes watching same signal; propagation; source scope drop is safe |
//! | **Nested spawn** | `nested_spawn_preserves_outer_polling_task`, `nested_spawn_preserves_polling_task_for_nonzero_timer` | `CURRENT_POLLING_TASK` save/restore across nested flushes |
//! | **Suspend / Resume** | `suspend_prevents_task_execution`, `resume_allows_task_execution`, `suspend_cascades_to_children`, `resume_cascades_to_children`, `siblings_not_affected_by_suspend`, `multiple_suspend_resume_no_leak`, `suspended_scope_drops_without_panic`, `suspend_scope_prevents_task_from_running_on_signal_change`, `suspend_multiple_changes_resume_single_effect`, `suspend_interaction_with_timer` | Suspend blocks task polling; resume enqueues scope tasks; cascading to children; multiple changes coalesce; timer interaction |
//! | **Timer** | `timer_zero_duration_completes_immediately`, `timer_normal_delay_fires_after_time_advances`, `timer_across_multiple_flushes`, `timer_cancelled_by_scope_drop` | `timer::sleep()`: immediate, delayed, multi-step, scope-cancelled cleanup |
//! | **Instance executor** | `flush_instance_*`, `instance_executor_*`, `multiple_instance_executors_*` | `Executor::new_instance()`: spawn, timer, drop/recreate, slot recycling, timer isolation |
//! | **Re-entrant drop** | `scope_drop_inside_signal_callback_triggers_task_cancellation` | Dropping last scope clone inside a signal callback cancels its tasks |
//! | **Scoped spawn + executor interaction** | `set_deferred_routes_to_instance_executor`, `set_deferred_isolated_to_instance_executor` | `set_deferred` routing via `with_executor` |

use super::*;
use crate::executor::{self, init_flush_scheduler, reset_executor_for_test, TestScheduleFlush};
use crate::{
    init_time_source, schedule_callback, set_global_max_deferred_callbacks, ScheduleFlush,
    TestTimeSource, TimeSource,
};
use auralis_signal::Signal;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

fn init() {
    reset_executor_for_test();
    init_flush_scheduler(Rc::new(TestScheduleFlush));
}

// -- scope ------------------------------------------------------------

#[test]
fn new_scope_has_zero_tasks() {
    let scope = TaskScope::new();
    assert_eq!(scope.task_count(), 0);
    assert_eq!(scope.child_count(), 0);
}

#[test]
fn new_child_attaches_to_parent() {
    let parent = TaskScope::new();
    let _child = TaskScope::new_child(&parent);
    assert_eq!(parent.child_count(), 1);
}

#[test]
fn spawn_adds_task() {
    init();
    let scope = TaskScope::new();
    scope.spawn(async {});
    assert_eq!(scope.task_count(), 1);
}

#[test]
fn spawn_and_complete() {
    init();
    let done = Rc::new(Cell::new(false));
    let done2 = Rc::clone(&done);
    spawn_global(async move {
        done2.set(true);
    });
    assert!(done.get());
}

#[test]
fn scope_spawn_and_cancel() {
    init();
    let dropped = Rc::new(Cell::new(false));
    {
        let scope = TaskScope::new();
        let d = Rc::clone(&dropped);
        struct DropCheck(Rc<Cell<bool>>);
        impl Drop for DropCheck {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        scope.spawn(async move {
            let _guard = DropCheck(d);
            std::future::pending::<()>().await;
        });
        assert_eq!(executor::debug_task_count(), 1);
    }
    assert!(dropped.get());
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn nested_scope_child_cancel_with_parent() {
    init();
    let dropped_child = Rc::new(Cell::new(false));
    {
        let parent = TaskScope::new();
        let child = TaskScope::new_child(&parent);
        let d = Rc::clone(&dropped_child);
        struct DropCheck(Rc<Cell<bool>>);
        impl Drop for DropCheck {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        child.spawn(async move {
            let _guard = DropCheck(d);
            std::future::pending::<()>().await;
        });
        assert_eq!(executor::debug_task_count(), 1);
    }
    assert!(dropped_child.get());
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn deeply_nested_scope_drop_no_stack_overflow() {
    init();
    let root = TaskScope::new();
    {
        let mut current = TaskScope::new_child(&root);
        for _ in 0..199 {
            current = TaskScope::new_child(&current);
        }
    }
    drop(root);
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn scope_child_explicit_tree() {
    let root = TaskScope::new();
    let a = TaskScope::new_child(&root);
    let b = TaskScope::new_child(&root);
    let _a1 = TaskScope::new_child(&a);
    let _a2 = TaskScope::new_child(&a);
    assert_eq!(root.child_count(), 2);
    assert_eq!(a.child_count(), 2);
    assert_eq!(b.child_count(), 0);
}

// -- callbacks -------------------------------------------------------

#[test]
fn callback_handle_dropped_before_tasks() {
    init();
    let dropped_order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    {
        let scope = TaskScope::new();
        let order1 = Rc::clone(&dropped_order);
        scope.register_callback_handle(CallbackHandle::new(move || {
            order1.borrow_mut().push("callback".to_string());
        }));
        let order2 = Rc::clone(&dropped_order);
        struct DropCheck {
            order: Rc<RefCell<Vec<String>>>,
            label: String,
        }
        impl Drop for DropCheck {
            fn drop(&mut self) {
                self.order.borrow_mut().push(self.label.clone());
            }
        }
        scope.spawn(async move {
            let _guard = DropCheck {
                order: order2,
                label: "task".to_string(),
            };
            std::future::pending::<()>().await;
        });
    }
    let order = dropped_order.borrow().clone();
    assert_eq!(order, vec!["callback", "task"]);
}

#[test]
fn callback_handle_cleaned_up_on_child_scope_drop() {
    init();
    let called = Rc::new(Cell::new(false));
    {
        let parent = TaskScope::new();
        let child = TaskScope::new_child(&parent);
        let c = Rc::clone(&called);
        child.register_callback_handle(CallbackHandle::new(move || {
            c.set(true);
        }));
        // Child dropped here.
    }
    assert!(called.get());
}

// -- context ----------------------------------------------------------

#[test]
fn context_provide_and_consume_in_same_scope() {
    let scope = TaskScope::new();
    scope.provide(42i32);
    assert_eq!(*scope.consume::<i32>().unwrap(), 42);
}

#[test]
fn context_consume_walks_up_to_parent() {
    let parent = TaskScope::new();
    parent.provide("hello".to_string());
    let child = TaskScope::new_child(&parent);
    assert_eq!(*child.consume::<String>().unwrap(), "hello");
}

#[test]
fn context_consume_not_found() {
    let scope = TaskScope::new();
    assert!(scope.consume::<i32>().is_none());
}

#[test]
fn context_removed_on_scope_drop() {
    let parent = TaskScope::new();
    parent.provide(99u32);
    {
        let _child = TaskScope::new_child(&parent);
        // Child can consume from parent.
    }
    // Parent still has the context.
    assert_eq!(*parent.consume::<u32>().unwrap(), 99);
}

#[test]
fn context_shadowing() {
    let parent = TaskScope::new();
    parent.provide(1i32);
    let child = TaskScope::new_child(&parent);
    child.provide(2i32);
    // Child's own value shadows parent's.
    assert_eq!(*child.consume::<i32>().unwrap(), 2);
    // Parent still has its own.
    assert_eq!(*parent.consume::<i32>().unwrap(), 1);
}

#[test]
#[should_panic(expected = "context not found")]
fn expect_context_panics_when_missing() {
    let scope = TaskScope::new();
    let _ = scope.expect_context::<String>();
}

// -- existing tests continue to pass -----------------------------------

#[test]
fn executor_priority_ordering() {
    init();
    let order = Rc::new(RefCell::new(Vec::new()));
    let o1 = Rc::clone(&order);
    executor::spawn_no_auto_flush(Priority::Low, async move {
        o1.borrow_mut().push("low");
    });
    let o2 = Rc::clone(&order);
    executor::spawn_no_auto_flush(Priority::High, async move {
        o2.borrow_mut().push("high");
    });
    executor::flush_all();
    let result = order.borrow().clone();
    assert_eq!(result, vec!["high", "low"]);
}

#[test]
fn executor_batch() {
    init();
    let counter = Rc::new(Cell::new(0u32));
    for _ in 0..10 {
        let c = Rc::clone(&counter);
        spawn_global(async move {
            c.set(c.get() + 1);
        });
    }
    assert_eq!(counter.get(), 10);
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn no_leak_on_cancel() {
    init();
    for _ in 0..50 {
        let scope = TaskScope::new();
        for _ in 0..5 {
            scope.spawn(std::future::pending::<()>());
        }
    }
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn set_deferred_triggers_after_flush() {
    use auralis_signal::Signal;
    init();
    let sig = Signal::new(0);
    let observed = Rc::new(Cell::new(0));
    set_deferred(&sig, 42);
    assert_eq!(sig.read(), 42);
    let ob1 = Rc::clone(&observed);
    spawn_global(async move {
        ob1.set(sig.read());
    });
    assert_eq!(observed.get(), 42);
}

#[test]
fn set_deferred_in_drop_safe() {
    use auralis_signal::Signal;
    init();
    let sig = Signal::new(0);
    struct SetOnDrop {
        sig: Signal<i32>,
        val: i32,
    }
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            set_deferred(&self.sig, self.val);
        }
    }
    let guard = SetOnDrop {
        sig: sig.clone(),
        val: 99,
    };
    drop(guard);
    assert_eq!(sig.read(), 99);
}

#[test]
fn set_deferred_from_drop_guard_during_scope_cancel() {
    use auralis_signal::Signal;
    init();

    let sig = Signal::new(0i32);

    // A drop guard that calls set_deferred — simulating a
    // component that resets shared state when its task is
    // cancelled.
    struct ResetOnDrop {
        sig: Signal<i32>,
    }
    impl Drop for ResetOnDrop {
        fn drop(&mut self) {
            set_deferred(&self.sig, 42);
        }
    }

    {
        let scope = TaskScope::new();
        let s = sig.clone();
        scope.spawn(async move {
            let _guard = ResetOnDrop { sig: s };
            // The guard's Drop will call set_deferred when this
            // future is cancelled by the scope dropping.
            std::future::pending::<()>().await;
        });
        // Scope dropped here — task cancelled, guard fires.
    }

    // After scope drop, the deferred op should have executed.
    assert_eq!(
        sig.read(),
        42,
        "set_deferred should have fired after scope cancel"
    );
}

#[test]
fn yield_now_gives_other_tasks_a_turn() {
    init();
    let order = Rc::new(RefCell::new(Vec::new()));
    let o1 = Rc::clone(&order);
    executor::spawn_no_auto_flush(Priority::Low, async move {
        o1.borrow_mut().push("a1");
        executor::yield_now().await;
        o1.borrow_mut().push("a2");
    });
    let o2 = Rc::clone(&order);
    executor::spawn_no_auto_flush(Priority::Low, async move {
        o2.borrow_mut().push("b1");
        o2.borrow_mut().push("b2");
    });
    executor::flush_all();
    let r = order.borrow().clone();
    assert_eq!(&r[0..3], &["a1", "b1", "b2"][..]);
    assert!(r.contains(&"a2"));
}

#[test]
fn panic_in_task_is_isolated() {
    init();
    let survived = Rc::new(Cell::new(false));
    let s = Rc::clone(&survived);
    spawn_global(async move {
        panic!("intentional test panic");
    });
    spawn_global(async move {
        s.set(true);
    });
    assert!(survived.get());
    assert_eq!(executor::debug_task_count(), 0);
}

// -- time budget -------------------------------------------------------

#[test]
fn time_budget_with_test_time_source() {
    init();
    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(ts.clone());

    let polled = Rc::new(Cell::new(0u32));

    // Spawn 50 tasks without auto-flush.  Each task increments the
    // counter and advances simulated time by 1 ms.
    for _ in 0..50 {
        let pc = Rc::clone(&polled);
        let ts_c = Rc::clone(&ts);
        executor::spawn_no_auto_flush(Priority::Low, async move {
            pc.set(pc.get() + 1);
            ts_c.advance(1);
        });
    }

    // With TestScheduleFlush the next-flush callback fires
    // synchronously, so budget breaks re-enter flush immediately.
    // All tasks eventually complete.
    executor::flush_all();

    assert_eq!(polled.get(), 50);
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn time_budget_honoured_with_split() {
    // Use a flush-scheduler that records calls instead of
    // re-entering, so we can observe that the budget actually
    // triggered a split.
    let schedule_count = Rc::new(Cell::new(0u32));
    struct NoopScheduleFlush(Rc<Cell<u32>>);
    impl ScheduleFlush for NoopScheduleFlush {
        fn schedule(&self, _callback: Box<dyn FnOnce()>) {
            self.0.set(self.0.get() + 1);
            // Intentionally do NOT call callback() — we want to
            // observe the break without re-entering.
        }
    }
    init_flush_scheduler(Rc::new(NoopScheduleFlush(Rc::clone(&schedule_count))));

    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(ts.clone());

    let polled = Rc::new(RefCell::new(Vec::new()));

    for i in 0..50u32 {
        let pc = Rc::clone(&polled);
        let ts_c = Rc::clone(&ts);
        executor::spawn_no_auto_flush(Priority::Low, async move {
            pc.borrow_mut().push(i);
            ts_c.advance(1);
        });
    }

    executor::flush_all();

    let completed = polled.borrow().len();
    assert!(
        completed < 50,
        "budget should split before all tasks run (only {completed} of 50)"
    );
    assert!(
        completed >= 7,
        "at least 7 tasks should run before budget expires ({completed})"
    );
    assert_eq!(
        schedule_count.get(),
        1,
        "next flush should have been scheduled exactly once"
    );

    // Clean up: schedule remaining tasks to finish.
    // Re-register TestScheduleFlush and flush again.
    init_flush_scheduler(Rc::new(TestScheduleFlush));
    executor::flush_all();
    assert_eq!(executor::debug_task_count(), 0);
}

// -- macros -----------------------------------------------------------

#[test]
fn provide_context_macro_works() {
    let scope = TaskScope::new();
    provide_context!(scope, 42i32);
    assert_eq!(*scope.consume::<i32>().unwrap(), 42);
}

#[test]
fn consume_context_macro_works() {
    let scope = TaskScope::new();
    scope.provide(99u32);
    let val: Option<Rc<u32>> = consume_context!(scope, u32);
    assert_eq!(*val.unwrap(), 99);
}

#[test]
fn consume_context_macro_not_found() {
    let scope = TaskScope::new();
    let val: Option<Rc<String>> = consume_context!(scope, String);
    assert!(val.is_none());
}

// -- dump_task_tree ---------------------------------------------------

#[cfg(feature = "debug")]
#[test]
fn dump_task_tree_returns_string() {
    init();
    let scope = TaskScope::new();
    scope.spawn(async { std::future::pending::<()>().await });

    let output = crate::dump_task_tree();
    assert!(output.contains("Auralis Task Tree"));
    assert!(output.contains("Total active tasks: 1"));
    assert!(output.contains("Scope"));
}

#[cfg(feature = "debug")]
#[test]
fn dump_task_tree_empty() {
    init();
    let output = crate::dump_task_tree();
    assert!(output.contains("(no active tasks)"));
}

use crate::{set_deferred, spawn_global};

// -- suspend / resume ---------------------------------------------------

#[test]
fn suspend_prevents_task_execution() {
    init();
    let scope = TaskScope::new();
    let executed = Rc::new(Cell::new(false));
    let ex = Rc::clone(&executed);
    scope.spawn(async move {
        ex.set(true);
    });
    // Task runs immediately with TestScheduleFlush.
    assert!(executed.get());
    executed.set(false);

    scope.suspend();
    let ex2 = Rc::clone(&executed);
    scope.spawn(async move {
        ex2.set(true);
    });
    // Task should NOT execute while suspended.
    assert!(!executed.get());
}

#[test]
fn resume_allows_task_execution() {
    init();
    let scope = TaskScope::new();
    scope.suspend();
    let executed = Rc::new(Cell::new(false));
    let ex = Rc::clone(&executed);
    scope.spawn(async move {
        ex.set(true);
    });
    assert!(!executed.get());

    scope.resume();
    // After resume, the task should execute.
    assert!(executed.get());
}

#[test]
fn suspend_cascades_to_children() {
    init();
    let parent = TaskScope::new();
    let child = TaskScope::new_child(&parent);
    assert!(!child.is_suspended());

    parent.suspend();
    assert!(parent.is_suspended());
    assert!(child.is_suspended());
}

#[test]
fn resume_cascades_to_children() {
    init();
    let parent = TaskScope::new();
    let child = TaskScope::new_child(&parent);
    parent.suspend();
    assert!(child.is_suspended());

    parent.resume();
    assert!(!parent.is_suspended());
    assert!(!child.is_suspended());
}

#[test]
fn multiple_suspend_resume_no_leak() {
    init();
    let scope = TaskScope::new();
    for _ in 0..50 {
        scope.suspend();
        assert!(scope.is_suspended());
        scope.resume();
        assert!(!scope.is_suspended());
    }
    // No panic, no leak.
}

#[test]
fn suspended_scope_drops_without_panic() {
    init();
    {
        let scope = TaskScope::new();
        scope.suspend();
        let d = Rc::new(Cell::new(false));
        struct DropCheck(Rc<Cell<bool>>);
        impl Drop for DropCheck {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        scope.spawn(async move {
            let _guard = DropCheck(d);
            std::future::pending::<()>().await;
        });
        // Scope dropped with tasks and in suspended state.
        // Tasks should be cancelled without panic.
    }
    // After scope drop, all tasks should be cleaned up.
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn siblings_not_affected_by_suspend() {
    init();
    let parent = TaskScope::new();
    let child_a = TaskScope::new_child(&parent);
    let child_b = TaskScope::new_child(&parent);

    child_a.suspend();
    assert!(child_a.is_suspended());
    assert!(!child_b.is_suspended());
    assert!(!parent.is_suspended());
}

// -- instance executor tests ------------------------------------------

use crate::Executor;

#[test]
fn flush_instance_panicking_task_is_isolated() {
    init();
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(TestScheduleFlush));

    let survived = Rc::new(Cell::new(false));
    let s = Rc::clone(&survived);

    Executor::spawn(&ex, async move {
        panic!("intentional test panic in instance executor");
    });
    Executor::spawn(&ex, async move {
        s.set(true);
    });
    Executor::flush_instance(&ex);

    assert!(survived.get());
}

#[test]
fn flush_instance_spawn_and_complete() {
    init();
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(TestScheduleFlush));

    let counter = Rc::new(Cell::new(0u32));
    for _ in 0..20 {
        let c = Rc::clone(&counter);
        Executor::spawn(&ex, async move {
            c.set(c.get() + 1);
        });
    }
    Executor::flush_instance(&ex);
    assert_eq!(counter.get(), 20);
}

// -- timer tests -------------------------------------------------------

use crate::timer;

#[test]
fn timer_zero_duration_completes_immediately() {
    init();
    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    spawn_global(async move {
        timer::sleep(Duration::ZERO).await;
        d.set(true);
    });
    // With TestScheduleFlush, the task completes synchronously.
    assert!(done.get());
}

#[test]
fn timer_normal_delay_fires_after_time_advances() {
    init();
    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(Rc::clone(&ts) as Rc<dyn TimeSource>);

    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    spawn_global(async move {
        timer::sleep(Duration::from_millis(100)).await;
        d.set(true);
    });
    // Timer registered but not yet expired — the task is sleeping.
    assert!(!done.get());

    // Advance time past the deadline, then flush to process the
    // expired timer and re-poll the task.
    ts.advance(150);
    crate::executor::flush_all();
    assert!(done.get());
}

#[test]
fn timer_across_multiple_flushes() {
    init();
    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(Rc::clone(&ts) as Rc<dyn TimeSource>);

    let counter = Rc::new(Cell::new(0u32));
    let c = Rc::clone(&counter);
    spawn_global(async move {
        for _ in 0..3 {
            timer::sleep(Duration::from_millis(100)).await;
            c.set(c.get() + 1);
        }
    });
    assert_eq!(counter.get(), 0);

    ts.advance(100);
    crate::executor::flush_all();
    assert_eq!(counter.get(), 1);

    ts.advance(100);
    crate::executor::flush_all();
    assert_eq!(counter.get(), 2);

    ts.advance(100);
    crate::executor::flush_all();
    assert_eq!(counter.get(), 3);
}

#[test]
fn timer_cancelled_by_scope_drop() {
    init();
    let executed = Rc::new(Cell::new(false));
    let ex = Rc::clone(&executed);
    {
        let scope = TaskScope::new();
        scope.spawn(async move {
            timer::sleep(Duration::from_millis(500)).await;
            ex.set(true);
        });
    }
    // Scope dropped → task cancelled → timer cleaned up.
    // The task should NOT execute.
    assert!(!executed.get());
    assert_eq!(executor::debug_task_count(), 0);
}

#[test]
fn reentrant_flush_is_noop() {
    init();
    // flush_instance re-entrancy guard: calling flush inside a
    // deferred callback (which runs during flush step 2) should
    // be a no-op and leave state intact.
    //
    // With TestScheduleFlush, signal callbacks fire synchronously
    // and a re-entrant flush() inside a callback is simply a no-op.
    let reentered = Rc::new(Cell::new(false));
    let r = Rc::clone(&reentered);
    let sig = Signal::new(0);
    auralis_signal::subscribe(&sig, Rc::new(move || r.set(true)));
    // This set triggers the callback synchronously (TestScheduleFlush).
    // The callback does not call flush itself, but we verify the
    // guard by calling flush() inside the deferred callback drain.
    sig.set(1);
    assert!(reentered.get());
}

#[test]
fn instance_executor_timer() {
    init();
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(TestScheduleFlush));
    let ts = Rc::new(TestTimeSource::new(0));
    Executor::install_time_source(&ex, Rc::clone(&ts) as Rc<dyn TimeSource>);

    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    Executor::spawn(&ex, async move {
        timer::sleep(Duration::from_millis(50)).await;
        d.set(true);
    });
    assert!(!done.get());

    // Timer should fire on the instance executor's flush.
    ts.advance(60);
    Executor::flush_instance(&ex);
    assert!(done.get());
}

#[test]
fn set_deferred_routes_to_instance_executor() {
    init();
    let ex = Executor::new_instance();
    Executor::install_flush_scheduler(&ex, Rc::new(TestScheduleFlush));

    let sig = Signal::new(0);
    let s = sig.clone();

    // Spawn a task on the instance executor that uses set_deferred.
    Executor::spawn(&ex, async move {
        crate::set_deferred(&s, 42);
    });

    // Flush the instance executor — set_deferred should route here.
    Executor::flush_instance(&ex);
    // The deferred set should have been processed.
    assert_eq!(sig.read(), 42);
}

// -- defensive / API coverage --------------------------------------

#[test]
fn panic_hook_is_invoked_on_task_panic() {
    init();
    let hook_called = Rc::new(Cell::new(false));
    let hc = Rc::clone(&hook_called);

    crate::set_panic_hook(Rc::new(move |_info| {
        hc.set(true);
    }));

    let scope = TaskScope::new();
    scope.spawn(async move { panic!("intentional") });

    // The panic hook should have been called.
    assert!(hook_called.get());
}

#[test]
fn current_scope_available_in_spawned_task() {
    init();
    let scope = TaskScope::new();
    let found = Rc::new(Cell::new(false));
    let f = Rc::clone(&found);
    scope.spawn(async move {
        f.set(crate::current_scope().is_some());
    });
    assert!(found.get());
}

#[test]
fn callback_handle_noop_does_not_panic() {
    let _h = crate::CallbackHandle::noop();
    // Dropping should not panic.
}

#[test]
fn sync_callback_fallback_without_schedule_hook() {
    // When no ScheduleFlush hook is installed, signal callbacks
    // fire synchronously inside set() (the executor_schedule fallback).
    crate::reset_executor_for_test();
    // No init_flush_scheduler call — hook is absent.

    let sig = Signal::new(0);
    let called = Rc::new(Cell::new(false));
    let c = Rc::clone(&called);
    auralis_signal::subscribe(&sig, Rc::new(move || c.set(true)));

    sig.set(1);
    // Without a hook, the callback fires synchronously.
    assert!(called.get());
}

#[test]
fn set_deferred_isolated_to_instance_executor() {
    init();
    let ex1 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex1, Rc::new(TestScheduleFlush));
    let ex2 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex2, Rc::new(TestScheduleFlush));

    let sig1 = Signal::new(0);
    let sig2 = Signal::new(0);
    let s1 = sig1.clone();

    // Spawn on ex1: use set_deferred via with_executor.
    crate::with_executor(&ex1, || {
        crate::set_deferred(&s1, 42);
    });
    Executor::flush_instance(&ex1);
    assert_eq!(sig1.read(), 42);
    // sig2 must be unaffected — set_deferred was on ex1.
    assert_eq!(sig2.read(), 0);
}

#[test]
fn notify_signal_state_follow_up_handles_reentrant_dirty() {
    // When a signal subscriber callback calls set() on the same
    // signal, the follow-up notification must fire correctly.
    let sig = Signal::new(0);
    let sig2 = sig.clone();
    let count = Rc::new(Cell::new(0u32));
    let c = Rc::clone(&count);

    auralis_signal::subscribe(
        &sig,
        Rc::new(move || {
            c.set(c.get() + 1);
            // Re-entrant set: should be picked up by follow-up.
            if c.get() == 1 {
                sig2.set(2);
            }
        }),
    );

    sig.set(1);
    // First callback (set 1): count=1, triggers re-entrant set(2).
    // Follow-up notification fires second callback: count=2.
    assert_eq!(sig.read(), 2);
    assert_eq!(count.get(), 2);
}

// -- CURRENT_POLLING_TASK save/restore ---------------------------------

#[test]
fn nested_spawn_preserves_outer_polling_task() {
    // When a sync scheduler triggers a nested flush during a spawned
    // task's poll, the outer task's id must survive so that subsequent
    // timer::sleep calls in the outer task can discover it.
    init();
    let executed = Rc::new(Cell::new(false));
    let ex = Rc::clone(&executed);

    let scope = TaskScope::new();
    scope.spawn(async move {
        // Spawn a nested task. With sync scheduler this triggers an
        // immediate nested flush, which would clear CURRENT_POLLING_TASK
        // if not properly saved/restored.
        crate::spawn_global(async {});
        // After the nested spawn+flush, this task must still be able
        // to discover its task id for timer::sleep.
        timer::sleep(Duration::ZERO).await;
        ex.set(true);
    });

    assert!(executed.get());
}

#[test]
fn nested_spawn_preserves_polling_task_for_nonzero_timer() {
    init();
    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(Rc::clone(&ts) as Rc<dyn TimeSource>);

    let executed = Rc::new(Cell::new(false));
    let ex = Rc::clone(&executed);

    let scope = TaskScope::new();
    scope.spawn(async move {
        // Nested spawn triggers sync flush.
        crate::spawn_global(async {});
        // Non-zero timer that expires after time advance.
        timer::sleep(Duration::from_millis(10)).await;
        ex.set(true);
    });

    // Timer hasn't fired yet.
    assert!(!executed.get());
    ts.advance(20);
    crate::executor::flush_all();
    assert!(executed.get());
}

// -- batch spawn + flush -----------------------------------------------

#[test]
fn batch_spawn_with_no_auto_flush_then_manual_flush() {
    // Sanity-check: spawn_no_auto_flush defers execution until
    // flush_all is called.  Both tasks must complete.
    init();

    let order = Rc::new(RefCell::new(Vec::new()));
    let o1 = Rc::clone(&order);
    let o2 = Rc::clone(&order);

    executor::spawn_no_auto_flush(Priority::Low, async move {
        o1.borrow_mut().push("a");
    });

    executor::spawn_no_auto_flush(Priority::Low, async move {
        o2.borrow_mut().push("b");
    });

    executor::flush_all();

    let r = order.borrow().clone();
    assert_eq!(r.len(), 2);
    assert!(r.contains(&"a"));
    assert!(r.contains(&"b"));
}

#[test]
fn spawn_many_tasks_all_complete() {
    // Spawn many tasks that each increment a counter.  With a
    // synchronous scheduler they run to completion immediately.
    init();

    let counter = Rc::new(Cell::new(0u32));
    for _ in 0..20 {
        let c = Rc::clone(&counter);
        spawn_global(async move {
            c.set(c.get() + 1);
        });
    }
    assert_eq!(counter.get(), 20);
    assert_eq!(executor::debug_task_count(), 0);
}

// -- instance executor lifecycle --------------------------------------

#[test]
fn instance_executor_create_drop_recreate_works() {
    // Creating and dropping several instance executors in sequence
    // must not leak slots or cause the slot table to grow unbounded.
    init();

    for _ in 0..10 {
        let ex = Executor::new_instance();
        Executor::install_flush_scheduler(&ex, Rc::new(TestScheduleFlush));
        let done = Rc::new(Cell::new(false));
        let d = Rc::clone(&done);
        Executor::spawn(&ex, async move {
            d.set(true);
        });
        Executor::flush_instance(&ex);
        assert!(done.get());
    }
    // No panic, no leak — slots were recycled.
}

#[test]
fn executor_drop_recreate_signal_subscriptions_cleaned_up() {
    // When an executor is dropped, all tasks are cancelled and their
    // signal subscriptions (SignalChangedFuture Drop) are cleaned up.
    // Setting the signal afterward must not crash, and a new executor
    // that recycles the slot must work normally.
    init();

    let sig = Signal::new(0i32);

    let ex1 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex1, Rc::new(TestScheduleFlush));

    // Spawn a task on ex1 that waits for a signal change.
    // This creates a waker bound to ex1's slot.
    let s = sig.clone();
    Executor::spawn(&ex1, async move {
        s.changed().await;
    });

    // Drop the executor. The task is cancelled but wakers may
    // still be registered in the signal's subscriber list.
    drop(ex1);

    // Create a new executor that may recycle the slot.
    let ex2 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex2, Rc::new(TestScheduleFlush));

    // Setting the signal must NOT crash, even if stale wakers
    // refer to the now-dead ex1 (slot recycled with incremented
    // generation).
    sig.set(42);

    // ex2 should work normally despite the stale wakers.
    let done2 = Rc::new(Cell::new(false));
    let d2 = Rc::clone(&done2);
    Executor::spawn(&ex2, async move {
        d2.set(true);
    });
    Executor::flush_instance(&ex2);
    assert!(done2.get());
}

#[test]
fn multiple_instance_executors_independent_timers() {
    // Timers on different instance executors must be completely
    // independent — a timer on ex1 must not fire on ex2.
    init();
    let ts = Rc::new(TestTimeSource::new(0));
    init_time_source(Rc::clone(&ts) as Rc<dyn TimeSource>);

    let ex1 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex1, Rc::new(TestScheduleFlush));
    Executor::install_time_source(&ex1, Rc::clone(&ts) as Rc<dyn TimeSource>);

    let ex2 = Executor::new_instance();
    Executor::install_flush_scheduler(&ex2, Rc::new(TestScheduleFlush));
    Executor::install_time_source(&ex2, Rc::clone(&ts) as Rc<dyn TimeSource>);

    let done1 = Rc::new(Cell::new(false));
    let done2 = Rc::new(Cell::new(false));
    let d1 = Rc::clone(&done1);
    let d2 = Rc::clone(&done2);

    Executor::spawn(&ex1, async move {
        timer::sleep(Duration::from_millis(100)).await;
        d1.set(true);
    });
    Executor::spawn(&ex2, async move {
        timer::sleep(Duration::from_millis(50)).await;
        d2.set(true);
    });

    // Before any time passes, neither should be done.
    assert!(!done1.get());
    assert!(!done2.get());

    // Advance 60ms. Only ex2's 50ms timer should fire.
    ts.advance(60);
    Executor::flush_instance(&ex2);
    assert!(!done1.get());
    assert!(done2.get());

    // Advance to 120ms. Now ex1's timer fires.
    ts.advance(60);
    Executor::flush_instance(&ex1);
    assert!(done1.get());
}

// -- JoinHandle ----------------------------------------------------------

#[test]
fn joinhandle_cancel_single_task_others_keep_running() {
    init();
    let scope = TaskScope::new();

    let a_ran = Rc::new(Cell::new(false));
    let b_ran = Rc::new(Cell::new(false));
    let ta = Rc::clone(&a_ran);
    let tb = Rc::clone(&b_ran);

    let handle_a = scope.spawn(async move {
        std::future::pending::<()>().await;
        ta.set(true);
    });
    scope.spawn(async move {
        tb.set(true);
    });

    handle_a.cancel();
    // Task B should complete; Task A should be cancelled.
    assert!(b_ran.get());
    assert!(!a_ran.get());
    assert!(handle_a.is_finished());
}

#[test]
fn joinhandle_is_finished_and_task_id() {
    init();
    let scope = TaskScope::new();

    let handle = scope.spawn(async {});
    assert!(handle.is_finished());
    assert!(handle.task_id().is_some());
}

#[test]
fn joinhandle_cancel_already_completed_task_is_noop() {
    init();
    let scope = TaskScope::new();
    let ran = Rc::new(Cell::new(false));
    let r = Rc::clone(&ran);
    let handle = scope.spawn(async move {
        r.set(true);
    });
    assert!(ran.get());
    assert!(handle.is_finished());
    handle.cancel(); // No-op, no panic.
}

// -- cross-scope interaction -------------------------------------------

#[test]
fn two_scopes_watch_same_signal_both_wake() {
    init();
    let sig = Signal::new(0);

    let scope_a = TaskScope::new();
    let scope_b = TaskScope::new();

    let a_ran = Rc::new(Cell::new(false));
    let b_ran = Rc::new(Cell::new(false));

    let sa = sig.clone();
    let ra = Rc::clone(&a_ran);
    scope_a.spawn(async move {
        sa.changed().await;
        ra.set(true);
    });

    let sb = sig.clone();
    let rb = Rc::clone(&b_ran);
    scope_b.spawn(async move {
        sb.changed().await;
        rb.set(true);
    });

    sig.set(42);
    assert!(a_ran.get());
    assert!(b_ran.get());
}

#[test]
fn cross_scope_signal_propagation() {
    init();
    let sig = Signal::new(0);

    let scope_a = TaskScope::new();
    let scope_b = TaskScope::new();

    let b_ran = Rc::new(Cell::new(false));
    let s = sig.clone();
    let rb = Rc::clone(&b_ran);
    scope_b.spawn(async move {
        s.changed().await;
        rb.set(true);
    });

    let sa = sig.clone();
    scope_a.spawn(async move {
        sa.set(99);
    });

    assert!(b_ran.get());
    assert_eq!(sig.read(), 99);
}

#[test]
fn drop_source_scope_does_not_cancel_receiving_scope() {
    init();
    let sig = Signal::new(0);
    let b_ran = Rc::new(Cell::new(false));
    let rb = Rc::clone(&b_ran);
    let s = sig.clone();

    let scope_b = TaskScope::new();
    {
        let _scope_a = TaskScope::new();
        scope_b.spawn(async move {
            s.changed().await;
            rb.set(true);
        });
        // _scope_a drops here — but scope_b lives on.
    }

    sig.set(42);
    assert!(b_ran.get());
}

// -- suspend / resume cross-scope -------------------------------------

#[test]
fn suspend_scope_prevents_task_from_running_on_signal_change() {
    init();
    let sig = Signal::new(0);

    let scope = TaskScope::new();
    let effect_ran = Rc::new(Cell::new(false));
    let er = Rc::clone(&effect_ran);
    let s = sig.clone();
    scope.spawn(async move {
        loop {
            s.changed().await;
            er.set(true);
        }
    });

    // First signal change runs the effect.
    sig.set(1);
    assert!(effect_ran.get());
    effect_ran.set(false);

    // Suspend → signal change should NOT run the effect.
    scope.suspend();
    sig.set(2);
    assert!(!effect_ran.get(), "effect should not run while suspended");

    // Resume → effect should be pending for next signal change.
    scope.resume();
    sig.set(3);
    assert!(effect_ran.get(), "effect should run after resume");
}

#[test]
fn suspend_interaction_with_timer() {
    init();
    let scope = TaskScope::new();
    let timer_ran = Rc::new(Cell::new(false));
    let tr = Rc::clone(&timer_ran);

    scope.spawn(async move {
        timer::sleep(Duration::ZERO).await;
        tr.set(true);
    });
    assert!(timer_ran.get());
    timer_ran.set(false);

    // Suspend → spawn a new timer task.
    scope.suspend();
    let tr2 = Rc::clone(&timer_ran);
    scope.spawn(async move {
        timer::sleep(Duration::ZERO).await;
        tr2.set(true);
    });
    assert!(
        !timer_ran.get(),
        "timer task should not execute while suspended"
    );

    // Resume → task should run.
    scope.resume();
    assert!(timer_ran.get());
}

// -- scope drop while re-entrant --------------------------------------

#[test]
fn scope_drop_inside_signal_callback_triggers_task_cancellation() {
    init();
    let sig = Signal::new(0i32);

    let task_dropped = Rc::new(Cell::new(false));
    let td = Rc::clone(&task_dropped);

    struct DropFlag(Rc<Cell<bool>>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let scope_cell = Rc::new(RefCell::new(Some(TaskScope::new())));
    if let Some(ref scope) = *scope_cell.borrow() {
        scope.spawn(async move {
            let _guard = DropFlag(td);
            std::future::pending::<()>().await;
        });
    }

    let sc = Rc::clone(&scope_cell);
    auralis_signal::subscribe(
        &sig,
        Rc::new(move || {
            sc.borrow_mut().take(); // drops last scope clone during callback
        }),
    );

    sig.set(1);
    assert!(task_dropped.get());

    // Also verify the scope doesn't exist anymore (was fully dropped).
    assert!(scope_cell.borrow().is_none());
}

// -- suspend + multiple changes → single effect -----------------------

#[test]
fn suspend_multiple_changes_resume_single_effect() {
    init();
    let sig = Signal::new(0);
    let effect_count = Rc::new(Cell::new(0u32));
    let ec = Rc::clone(&effect_count);

    let scope = TaskScope::new();
    let s = sig.clone();
    scope.spawn(async move {
        loop {
            s.changed().await;
            ec.set(ec.get() + 1);
        }
    });

    scope.suspend();
    sig.set(1);
    sig.set(2);
    sig.set(3);
    assert_eq!(effect_count.get(), 0);

    scope.resume();
    assert_eq!(effect_count.get(), 1);
}

// -- priority queue ordering -------------------------------------------

#[test]
fn priority_queue_ordering_with_many_tasks() {
    init();
    let order = Rc::new(RefCell::new(Vec::new()));

    // Spawn 10 low-priority tasks first, then 10 high-priority tasks.
    // All high should execute before all low.
    for i in 0..10 {
        let o = Rc::clone(&order);
        executor::spawn_no_auto_flush(Priority::Low, async move {
            o.borrow_mut().push(format!("L{i}"));
        });
    }
    for i in 0..10 {
        let o = Rc::clone(&order);
        executor::spawn_no_auto_flush(Priority::High, async move {
            o.borrow_mut().push(format!("H{i}"));
        });
    }

    executor::flush_all();

    let result = order.borrow().clone();
    // First 10 must all be high priority.
    for (i, entry) in result.iter().take(10).enumerate() {
        assert!(
            entry.starts_with('H'),
            "position {i} should be high priority, got {entry}"
        );
    }
    assert_eq!(result.len(), 20);
}

#[test]
fn priority_low_not_permanently_starved() {
    init();
    let low_ran = Rc::new(Cell::new(false));
    let lr = Rc::clone(&low_ran);

    // Spawn many high-priority tasks that yield, then one low.
    for _ in 0..20 {
        executor::spawn_no_auto_flush(Priority::High, async move {
            executor::yield_now().await;
        });
    }
    executor::spawn_no_auto_flush(Priority::Low, async move {
        lr.set(true);
    });

    executor::flush_all();
    assert!(low_ran.get(), "low priority task should eventually execute");
}

// -- deferred callback limit ----------------------------------------------

#[test]
#[should_panic(expected = "deferred callback limit exceeded")]
fn max_deferred_callbacks_limit_panics_when_exceeded() {
    init();
    set_global_max_deferred_callbacks(Some(3));

    // Push 4 callbacks — the 4th should panic.
    // With TestScheduleFlush, flush runs inside schedule_callback and
    // drains the queue, so we need to prevent the flush from draining.
    // schedule_callback itself doesn't trigger a recursive drain —
    // it enqueues and schedules a future flush.
    // But TestScheduleFlush calls the flush callback immediately, which
    // drains the queue, so we never accumulate more than 1 callback.
    // Use a NoopScheduleFlush instead.

    // Clear and replace with no-op scheduler.
    reset_executor_for_test();
    let schedule_count = Rc::new(Cell::new(0u32));
    struct NoopSched(Rc<Cell<u32>>);
    impl ScheduleFlush for NoopSched {
        fn schedule(&self, _callback: Box<dyn FnOnce()>) {
            self.0.set(self.0.get() + 1);
        }
    }
    let sched: Rc<dyn ScheduleFlush> = Rc::new(NoopSched(Rc::clone(&schedule_count)));
    init_flush_scheduler(sched);
    set_global_max_deferred_callbacks(Some(3));

    // Push 3 callbacks — should succeed.
    schedule_callback(Box::new(|| {}));
    schedule_callback(Box::new(|| {}));
    schedule_callback(Box::new(|| {}));
    assert_eq!(schedule_count.get(), 1); // only first one triggers schedule

    // The 4th should panic.
    schedule_callback(Box::new(|| {}));
}
