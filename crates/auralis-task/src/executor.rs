//! Single-threaded executor with priority scheduling, time-budget
//! awareness, and deferred-signal support.
//!
//! ## Architecture
//!
//! The executor is stored via a pluggable [`ExecutorStorage`] strategy
//! (defaulting to a per-thread slot).  Before polling a task the future
//! is **temporarily removed** so that the poll never holds an executor
//! borrow — this allows nested spawns, wakes, and `set_deferred` calls
//! without `RefCell` panics.
//!
//! The waker carries only a `task_id: u64`, making it trivially
//! [`Send`] + [`Sync`] for [`Waker::from`].

#![allow(clippy::cast_possible_truncation)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use auralis_signal::Signal;

use crate::Priority;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type TaskId = u64;

// ---------------------------------------------------------------------------
// ScheduleFlush
// ---------------------------------------------------------------------------

/// Platform hook for scheduling a microtask callback.
pub trait ScheduleFlush {
    /// Request that `callback` runs at the next microtask boundary.
    fn schedule(&self, callback: Box<dyn FnOnce()>);
}

/// A [`ScheduleFlush`] that fires the callback synchronously.
///
/// Makes the executor run-to-completion in unit tests without a browser
/// event loop.
#[cfg(test)]
pub struct TestScheduleFlush;

#[cfg(test)]
impl ScheduleFlush for TestScheduleFlush {
    fn schedule(&self, callback: Box<dyn FnOnce()>) {
        callback();
    }
}

// ---------------------------------------------------------------------------
// TimeSource
// ---------------------------------------------------------------------------

/// High-resolution time source for the executor's time-budget
/// accounting.
///
/// When registered via [`init_time_source`], the executor queries this
/// before and after each task poll to decide whether it should yield
/// control back to the host event loop (default budget: 8 ms).
///
/// In Wasm environments the implementation typically delegates to
/// `performance.now()`.  If no [`TimeSource`] is registered the time
/// budget check is a no-op and the executor runs tasks until the
/// queues are drained.
pub trait TimeSource {
    /// Return the current time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// A [`TimeSource`] whose value is explicitly controlled by the test.
///
/// Use [`set`](TestTimeSource::set) or [`advance`](TestTimeSource::advance)
/// to simulate the passage of time during a flush cycle.
#[cfg(test)]
pub struct TestTimeSource {
    now: std::cell::Cell<u64>,
}

#[cfg(test)]
impl TestTimeSource {
    /// Create a new [`TestTimeSource`] with the given initial time.
    #[must_use]
    pub fn new(initial_ms: u64) -> Self {
        Self {
            now: std::cell::Cell::new(initial_ms),
        }
    }

    /// Set the current time to `ms` milliseconds.
    pub fn set(&self, ms: u64) {
        self.now.set(ms);
    }

    /// Advance the current time by `ms` milliseconds.
    pub fn advance(&self, ms: u64) {
        self.now.set(self.now.get() + ms);
    }
}

#[cfg(test)]
impl TimeSource for TestTimeSource {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
}

// ---------------------------------------------------------------------------
// TaskWaker
// ---------------------------------------------------------------------------

struct TaskWaker {
    task_id: TaskId,
    priority: Priority,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        let maybe_sched = EXECUTOR.with(|exec| {
            if let Ok(mut ex) = exec.try_borrow_mut() {
                match self.priority {
                    Priority::High => ex.high_queue.push_back(self.task_id),
                    Priority::Low => ex.low_queue.push_back(self.task_id),
                }
                // Only schedule a fresh flush if we're NOT already inside
                // one (the running flush loop will pick up the task).
                if ex.in_flush {
                    None
                } else {
                    ex.try_schedule_flush()
                }
            } else {
                PENDING_WAKES.with(|pw| {
                    pw.borrow_mut().push((self.task_id, self.priority));
                });
                None
            }
        });
        if let Some(sched) = maybe_sched {
            sched.schedule(Box::new(flush));
        }
    }
}

// ---------------------------------------------------------------------------
// TaskState
// ---------------------------------------------------------------------------

struct TaskState {
    future: Pin<Box<dyn Future<Output = ()> + 'static>>,
    priority: Priority,
    scope_id: u64,
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Information about a task panic, passed to the user-registered
/// [`set_panic_hook`].
#[derive(Debug)]
pub struct PanicInfo {
    /// The executor-assigned task id.
    pub task_id: u64,
    /// The scope that owned the task (0 for global tasks).
    pub scope_id: u64,
    /// The boxed panic payload.
    pub payload: Box<dyn std::any::Any + Send>,
}

/// A single-threaded async task executor with priority queues.
///
/// Each [`Executor`] manages its own task slots, ready queues, and
/// deferred callback buffers.  Use [`Executor::new_instance`] to create
/// an isolated executor (e.g. per SSR request), or use the global
/// thread-local executor via [`spawn_global`](crate::spawn_global).
pub struct Executor {
    high_queue: VecDeque<TaskId>,
    low_queue: VecDeque<TaskId>,
    tasks: Vec<Option<TaskState>>,
    free_slots: Vec<TaskId>,
    next_task_id: TaskId,
    is_flush_scheduled: bool,
    in_flush: bool,
    deferred_ops: Vec<DeferredOp>,
    /// Callbacks pushed by `Signal::set` via the schedule hook.
    /// Drained at the start of every flush before polling tasks.
    deferred_callbacks: Vec<Box<dyn FnOnce()>>,
    flush_scheduler: Option<Rc<dyn ScheduleFlush>>,
    time_source: Option<Rc<dyn TimeSource>>,
    /// Maximum milliseconds to spend inside a single flush before
    /// yielding back to the host event loop.  Default: 8 ms.
    time_budget_ms: u64,
    /// Optional hook invoked when a spawned task panics.
    panic_hook: Option<Rc<dyn Fn(PanicInfo)>>,
    /// Timer queue: map from deadline (ms) to task ids that should be
    /// woken when that deadline expires.  Processed at the start of
    /// every flush.
    timers: BTreeMap<u64, Vec<TaskId>>,
}

// Set by the executor before polling a task, cleared afterward.
// Lets futures discover their task id without threading it through
// layers of combinators.
thread_local! {
    static CURRENT_POLLING_TASK: Cell<Option<TaskId>> = const { Cell::new(None) };
}

pub(crate) fn with_current_polling_task<R>(f: impl FnOnce(Option<TaskId>) -> R) -> R {
    CURRENT_POLLING_TASK.with(|c| f(c.get()))
}

struct DeferredOp {
    f: Box<dyn FnOnce()>,
}

impl Executor {
    fn new() -> Self {
        Self {
            high_queue: VecDeque::new(),
            low_queue: VecDeque::new(),
            tasks: Vec::new(),
            free_slots: Vec::new(),
            next_task_id: 0,
            is_flush_scheduled: false,
            in_flush: false,
            deferred_ops: Vec::new(),
            deferred_callbacks: Vec::new(),
            flush_scheduler: None,
            time_source: None,
            time_budget_ms: 8,
            panic_hook: None,
            timers: BTreeMap::new(),
        }
    }

    fn allocate_id(&mut self) -> TaskId {
        if let Some(id) = self.free_slots.pop() {
            return id;
        }
        let id = self.next_task_id;
        self.next_task_id += 1;
        self.tasks.push(None);
        id
    }

    fn free_slot(&mut self, task_id: TaskId) {
        self.tasks[task_id as usize] = None;
        self.free_slots.push(task_id);
    }

    fn enqueue(&mut self, task_id: TaskId) {
        let priority = match self.tasks.get(task_id as usize).and_then(Option::as_ref) {
            Some(t) => t.priority,
            None => return,
        };
        match priority {
            Priority::High => self.high_queue.push_back(task_id),
            Priority::Low => self.low_queue.push_back(task_id),
        }
    }

    fn dequeue(&mut self) -> Option<TaskId> {
        self.high_queue
            .pop_front()
            .or_else(|| self.low_queue.pop_front())
    }

    /// Mark that a flush is needed and return the scheduler if one is
    /// registered.  The caller **must** invoke the scheduler **after**
    /// releasing the executor borrow.
    fn try_schedule_flush(&mut self) -> Option<Rc<dyn ScheduleFlush>> {
        if self.is_flush_scheduled {
            return None;
        }
        self.is_flush_scheduled = true;
        self.flush_scheduler.clone()
    }

    /// Return the current time in ms, or 0 if no [`TimeSource`] is
    /// registered.  When this returns 0 the time-budget check is
    /// effectively a no-op.
    pub(crate) fn now_ms(&self) -> u64 {
        self.time_source.as_ref().map_or(0, |ts| ts.now_ms())
    }

    /// Return the number of currently active (not-yet-completed) tasks.
    ///
    /// Used by streaming SSR to determine whether the stream should
    /// wait for more work or terminate.
    #[must_use]
    pub fn active_task_count(&self) -> usize {
        self.tasks.iter().filter(|t| t.is_some()).count()
    }
}

// ---------------------------------------------------------------------------
// Thread-local globals (default storage)
// ---------------------------------------------------------------------------

thread_local! {
    static EXECUTOR: Rc<RefCell<Executor>> = Rc::new(RefCell::new(Executor::new()));
    static PENDING_WAKES: RefCell<Vec<(TaskId, Priority)>> = const { RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------
// Executor instance methods (for isolated executors, e.g. SSR)
// ---------------------------------------------------------------------------

impl Executor {
    /// Create a new isolated executor, wrapped for shared access.
    ///
    /// The returned executor is independent of the global thread-local
    /// executor.  Use [`with_executor`] to make it the current executor
    /// for the duration of a closure, so that spawned tasks and signal
    /// callbacks are routed to it.
    #[must_use]
    pub fn new_instance() -> Rc<RefCell<Executor>> {
        Rc::new(RefCell::new(Executor::new()))
    }

    /// Install a flush scheduler on this executor instance.
    pub fn install_flush_scheduler(ex: &Rc<RefCell<Executor>>, sched: Rc<dyn ScheduleFlush>) {
        ex.borrow_mut().flush_scheduler = Some(sched);
    }

    /// Install a time source on this executor instance.
    pub fn install_time_source(ex: &Rc<RefCell<Executor>>, ts: Rc<dyn TimeSource>) {
        ex.borrow_mut().time_source = Some(ts);
    }

    /// Set the maximum time (in milliseconds) a single flush may spend
    /// before yielding back to the host event loop.
    ///
    /// The default is 8 ms.  Set to `u64::MAX` to disable time-budget
    /// yielding (flush runs to completion).
    pub fn set_time_budget(ex: &Rc<RefCell<Executor>>, budget_ms: u64) {
        ex.borrow_mut().time_budget_ms = budget_ms;
    }

    /// Register a callback invoked whenever a spawned task panics.
    ///
    /// The default is no hook — panicking tasks are silently removed
    /// from the executor (the same behaviour as a task returning
    /// `Poll::Ready(())`).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// Executor::set_panic_hook(&ex, Rc::new(|info| {
    ///     eprintln!("task {} in scope {} panicked", info.task_id, info.scope_id);
    /// }));
    /// ```
    pub fn set_panic_hook(ex: &Rc<RefCell<Executor>>, hook: Rc<dyn Fn(PanicInfo)>) {
        ex.borrow_mut().panic_hook = Some(hook);
    }

    /// Register a timer: when `now_ms() >= deadline_ms`, enqueue
    /// `task_id` so it gets polled on the next flush.
    pub(crate) fn schedule_timer(ex: &Rc<RefCell<Executor>>, deadline_ms: u64, task_id: TaskId) {
        let mut e = ex.borrow_mut();
        e.timers.entry(deadline_ms).or_default().push(task_id);
        // Request a flush so the timer is checked.
        e.is_flush_scheduled = false;
        let maybe_sched = e.try_schedule_flush();
        drop(e);
        if let Some(sched) = maybe_sched {
            let ex2 = Rc::clone(ex);
            sched.schedule(Box::new(move || Self::flush_instance(&ex2)));
        }
    }

    /// Spawn a future on this executor instance.
    pub fn spawn(ex: &Rc<RefCell<Executor>>, future: impl Future<Output = ()> + 'static) {
        let maybe_sched = {
            let mut e = ex.borrow_mut();
            let tid = e.allocate_id();
            e.tasks[tid as usize] = Some(TaskState {
                future: Box::pin(future),
                priority: Priority::Low,
                scope_id: 0,
            });
            e.enqueue(tid);
            e.try_schedule_flush()
        };
        if let Some(sched) = maybe_sched {
            let ex2 = Rc::clone(ex);
            sched.schedule(Box::new(move || Self::flush_instance(&ex2)));
        }
    }

    /// Run a full flush cycle on this executor instance.
    ///
    /// Mirrors the global flush cycle but operates on an
    /// isolated executor (used for SSR).  Includes all the same
    /// protections: `catch_unwind`, suspend checks, time-budget
    /// yielding, and callback-drain budget.
    #[allow(clippy::too_many_lines)]
    pub fn flush_instance(ex: &Rc<RefCell<Executor>>) {
        // Guard against re-entrant flushes.
        {
            let mut e = ex.borrow_mut();
            if e.in_flush {
                #[cfg(debug_assertions)]
                {
                    eprintln!(
                        "[auralis-task] WARNING: Executor::flush_instance called \
                         re-entrantly (already inside a flush). This is a no-op. \
                         Check for nested flush() calls in signal callbacks or \
                         ScheduleFlush implementations."
                    );
                }
                return;
            }
            e.in_flush = true;
        }

        // Step 0: drain expired timers.
        {
            let mut e = ex.borrow_mut();
            let now = e.now_ms();
            if now > 0 {
                let expired: Vec<u64> =
                    e.timers.keys().copied().take_while(|&d| d <= now).collect();
                for deadline in expired {
                    if let Some(tasks) = e.timers.remove(&deadline) {
                        for tid in tasks {
                            e.enqueue(tid);
                        }
                    }
                }
            }
        }

        // Step 1: deferred ops.
        let deferred = std::mem::take(&mut ex.borrow_mut().deferred_ops);
        for op in deferred {
            (op.f)();
        }

        // Step 2: drain deferred signal callbacks with time budget.
        {
            let cb_start = ex.borrow().now_ms();
            loop {
                let callbacks = std::mem::take(&mut ex.borrow_mut().deferred_callbacks);
                if callbacks.is_empty() {
                    break;
                }
                for cb in callbacks {
                    cb();
                }
                if ex.borrow().now_ms().saturating_sub(cb_start) >= ex.borrow().time_budget_ms {
                    if !ex.borrow().deferred_callbacks.is_empty() {
                        let (sched, ex2) = {
                            let mut e = ex.borrow_mut();
                            e.in_flush = false;
                            e.is_flush_scheduled = false;
                            (e.try_schedule_flush(), Rc::clone(ex))
                        };
                        if let Some(sched) = sched {
                            sched.schedule(Box::new(move || Self::flush_instance(&ex2)));
                        }
                        return;
                    }
                    break;
                }
            }
        }

        // Step 3: main poll loop with time-budget check.
        let poll_start = ex.borrow().now_ms();
        loop {
            let task_id = ex.borrow_mut().dequeue();
            let Some(tid) = task_id else {
                let mut e = ex.borrow_mut();
                e.is_flush_scheduled = false;
                e.in_flush = false;
                break;
            };

            // Take the task out so the poll doesn't hold an executor borrow.
            let maybe_state = ex.borrow_mut().tasks[tid as usize].take();
            if let Some(mut state) = maybe_state {
                let priority = state.priority;
                let scope_id = state.scope_id;

                // Check if the owning scope is suspended.
                let scope = crate::scope::find_scope(scope_id);
                if let Some(ref s) = scope {
                    if s.is_suspended() {
                        let mut e = ex.borrow_mut();
                        if e.tasks[tid as usize].is_none() {
                            e.tasks[tid as usize] = Some(state);
                        }
                        continue;
                    }
                }

                let waker = Waker::from(Arc::new(TaskWaker {
                    task_id: tid,
                    priority,
                }));
                let mut cx = Context::from_waker(&waker);

                // Inject owning scope.
                let prev_scope = crate::scope::get_scope_direct();
                if scope.is_some() {
                    crate::scope::set_scope_direct(scope);
                }

                // Let futures discover their task id (used by timer::sleep).
                CURRENT_POLLING_TASK.with(|c| c.set(Some(tid)));

                // Task isolation (non-Wasm).
                #[cfg(not(target_arch = "wasm32"))]
                let result: Result<Poll<()>, Box<dyn std::any::Any + Send>> =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        state.future.as_mut().poll(&mut cx)
                    }));
                #[cfg(target_arch = "wasm32")]
                let poll = state.future.as_mut().poll(&mut cx);

                CURRENT_POLLING_TASK.with(|c| c.set(None));
                crate::scope::set_scope_direct(prev_scope);

                #[cfg(not(target_arch = "wasm32"))]
                {
                    match result {
                        Ok(Poll::Ready(())) => {
                            ex.borrow_mut().free_slot(tid);
                        }
                        Err(payload) => {
                            // Notify the panic hook (if any) before freeing the slot.
                            let hook = ex.borrow().panic_hook.clone();
                            if let Some(h) = hook {
                                h(PanicInfo {
                                    task_id: tid,
                                    scope_id,
                                    payload,
                                });
                            }
                            ex.borrow_mut().free_slot(tid);
                        }
                        Ok(Poll::Pending) => {
                            let mut e = ex.borrow_mut();
                            if e.tasks[tid as usize].is_none() {
                                e.tasks[tid as usize] = Some(state);
                            }
                        }
                    }
                }
                #[cfg(target_arch = "wasm32")]
                {
                    match poll {
                        Poll::Ready(()) => {
                            ex.borrow_mut().free_slot(tid);
                        }
                        Poll::Pending => {
                            let mut e = ex.borrow_mut();
                            if e.tasks[tid as usize].is_none() {
                                e.tasks[tid as usize] = Some(state);
                            }
                        }
                    }
                }
            }

            // Time budget check.
            {
                let elapsed = ex.borrow().now_ms().saturating_sub(poll_start);
                if elapsed >= ex.borrow().time_budget_ms {
                    let (maybe_sched, ex_clone) = {
                        let mut e = ex.borrow_mut();
                        e.is_flush_scheduled = false;
                        e.in_flush = false;
                        let sched = if !e.high_queue.is_empty() || !e.low_queue.is_empty() {
                            e.try_schedule_flush()
                        } else {
                            None
                        };
                        (sched, Rc::clone(ex))
                    };
                    if let Some(sched) = maybe_sched {
                        sched.schedule(Box::new(move || Self::flush_instance(&ex_clone)));
                    }
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Current-executor storage — injectable, defaults to thread-local
// ---------------------------------------------------------------------------

type ExecutorRef = Rc<RefCell<Executor>>;

thread_local! {
    static CURRENT_EXECUTOR: RefCell<Option<ExecutorRef>> = const { RefCell::new(None) };
}

/// Run `f` with `ex` set as the current executor.
///
/// Signal callbacks and `spawn_global` calls inside `f` will be routed
/// to `ex` instead of the global thread-local executor.  Restores the
/// previous executor afterward.
///
/// # Signal routing constraints
///
/// Auralis uses a **single global schedule hook** (installed once by the
/// first call to [`init_flush_scheduler`]) that decides where signal
/// notifications land by checking the current executor **at the time the
/// notification fires**, not at the time `Signal::set` is called.
///
/// This design implies two hard requirements for multi-instance users:
///
/// 1. **`init_flush_scheduler` must be called at least once** — without
///    it, `Signal::set` falls back to synchronous callback execution,
///    which breaks the deferred-notification model and can cause
///    re-entrant borrow panics.
/// 2. **The instance executor must still be "current" when the flush
///    runs** — if `with_executor` has already exited, deferred callbacks
///    from signals set inside `f` will be routed to the global executor
///    (or synchronously if no global hook is installed).
///
/// For the typical single-threaded case (Wasm, game loop, CLI), both
/// requirements are satisfied trivially: call `init_flush_scheduler`
/// once at startup and never use `with_executor`.  For SSR / multi-tenant
/// servers, ensure that `with_executor` wraps the entire request
/// lifecycle — from signal creation through the final flush.
///
/// # Example
///
/// ```rust,ignore
/// use auralis_task::Executor;
///
/// let ex = Executor::new_instance();
/// Executor::install_flush_scheduler(&ex, my_scheduler);
/// auralis_task::with_executor(&ex, || {
///     // Signal notifications and task spawns here go to `ex`.
/// });
/// ```
pub fn with_executor<R>(ex: &ExecutorRef, f: impl FnOnce() -> R) -> R {
    CURRENT_EXECUTOR.with(|exec| {
        let prev = exec.borrow_mut().replace(Rc::clone(ex));
        let result = f();
        *exec.borrow_mut() = prev;
        result
    })
}

/// Return the current executor, if any.
///
/// If no executor has been set via [`with_executor`], returns `None` —
/// callers should fall back to the global thread-local executor.
fn current_executor() -> Option<ExecutorRef> {
    CURRENT_EXECUTOR.with(|exec| exec.borrow().clone())
}

/// Return the currently active executor instance.
///
/// If [`with_executor`] was used to set an instance executor, returns
/// that; otherwise returns the global thread-local executor.
pub(crate) fn current_executor_instance() -> ExecutorRef {
    current_executor().unwrap_or_else(|| EXECUTOR.with(Rc::clone))
}

/// Return the current time in milliseconds from the active executor's
/// [`TimeSource`], or 0 if none is installed.
pub(crate) fn current_time_ms() -> u64 {
    current_executor_instance().borrow().now_ms()
}

// ---------------------------------------------------------------------------
// Helpers — use thread_local EXECUTOR
// ---------------------------------------------------------------------------

fn drain_pending_wakes() {
    PENDING_WAKES.with(|pw| {
        let wakes = std::mem::take(&mut *pw.borrow_mut());
        if wakes.is_empty() {
            return;
        }
        EXECUTOR.with(|exec| {
            let maybe_sched = {
                let mut ex = exec.borrow_mut();
                for (id, priority) in wakes {
                    match priority {
                        Priority::High => ex.high_queue.push_back(id),
                        Priority::Low => ex.low_queue.push_back(id),
                    }
                }
                ex.try_schedule_flush()
            };
            if let Some(sched) = maybe_sched {
                sched.schedule(Box::new(flush));
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

fn flush() {
    EXECUTOR.with(Executor::flush_instance);
    // Drain any wakes that landed in PENDING_WAKES because the executor
    // RefCell was borrowed during a callback or task poll.
    drain_pending_wakes();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Set the platform flush scheduler and install the signal deferred-
/// callback hook (idempotent — subsequent calls are no-ops for the hook).
pub fn init_flush_scheduler(sched: Rc<dyn ScheduleFlush>) {
    EXECUTOR.with(|exec| exec.borrow_mut().flush_scheduler = Some(sched));
    install_signal_hook_once();
}

/// Install the hook that bridges `auralis_signal::Signal::set` to the
/// executor's deferred-callback queue.
///
/// Idempotent — safe to call multiple times.
fn install_signal_hook_once() {
    use std::sync::OnceLock;
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        auralis_signal::install_schedule_hook(Box::new(|cb: Box<dyn FnOnce()>| {
            // Prefer the current executor (set via `with_executor`) for
            // SSR multi-request isolation; fall back to the global one.
            if let Some(ex) = current_executor() {
                let maybe_sched = {
                    let mut e = ex.borrow_mut();
                    e.deferred_callbacks.push(cb);
                    if e.in_flush {
                        None
                    } else {
                        e.try_schedule_flush()
                    }
                };
                if let Some(sched) = maybe_sched {
                    let ex2 = Rc::clone(&ex);
                    sched.schedule(Box::new(move || Executor::flush_instance(&ex2)));
                }
            } else {
                EXECUTOR.with(|exec| {
                    let maybe_sched = {
                        let mut ex = exec.borrow_mut();
                        ex.deferred_callbacks.push(cb);
                        if ex.in_flush {
                            None
                        } else {
                            ex.try_schedule_flush()
                        }
                    };
                    if let Some(sched) = maybe_sched {
                        sched.schedule(Box::new(flush));
                    }
                });
            }
        }));
    });
}

/// Set the platform time source used for time-budget accounting.
///
/// If no [`TimeSource`] is registered the executor runs every flush to
/// completion without yielding, which is acceptable for short-running
/// workloads but may cause frame drops in the browser.
pub fn init_time_source(ts: Rc<dyn TimeSource>) {
    EXECUTOR.with(|exec| exec.borrow_mut().time_source = Some(ts));
}

/// Set the per-flush time budget on the global executor.
///
/// See [`Executor::set_time_budget`] for details.
pub fn set_global_time_budget(budget_ms: u64) {
    EXECUTOR.with(|exec| exec.borrow_mut().time_budget_ms = budget_ms);
}

/// Register a global panic hook called when any globally-spawned
/// task panics.
///
/// See [`Executor::set_panic_hook`] for details.
pub fn set_panic_hook(hook: Rc<dyn Fn(PanicInfo)>) {
    EXECUTOR.with(|exec| exec.borrow_mut().panic_hook = Some(hook));
}

/// Remove the global panic hook, restoring the default silent
/// behaviour.
pub fn remove_panic_hook() {
    EXECUTOR.with(|exec| exec.borrow_mut().panic_hook = None);
}

/// Spawn a future on the global executor at low priority.
pub fn spawn_global(future: impl Future<Output = ()> + 'static) {
    spawn_global_with_priority(Priority::Low, future);
}

/// Spawn a future on the global executor at the given priority.
pub fn spawn_global_with_priority(priority: Priority, future: impl Future<Output = ()> + 'static) {
    spawn_inner(Box::pin(future), priority, 0);
}

pub(crate) fn spawn_scoped(
    priority: Priority,
    scope_id: u64,
    future: impl Future<Output = ()> + 'static,
) -> TaskId {
    spawn_inner(Box::pin(future), priority, scope_id)
}

fn spawn_inner(
    future: Pin<Box<dyn Future<Output = ()> + 'static>>,
    priority: Priority,
    scope_id: u64,
) -> TaskId {
    EXECUTOR.with(|exec| {
        let (task_id, maybe_sched) = {
            let mut ex = exec.borrow_mut();
            let task_id = ex.allocate_id();
            ex.tasks[task_id as usize] = Some(TaskState {
                future,
                priority,
                scope_id,
            });
            ex.enqueue(task_id);
            let sched = ex.try_schedule_flush();
            (task_id, sched)
        };
        // Schedule outside the borrow.
        if let Some(sched) = maybe_sched {
            sched.schedule(Box::new(flush));
        }
        task_id
    })
}

/// Enqueue all tasks belonging to `scope_id` and trigger a flush.
///
/// Used by [`TaskScope::resume`] to restart tasks after a suspend.
pub(crate) fn enqueue_scope_tasks(scope_id: u64) {
    EXECUTOR.with(|exec| {
        let task_ids: Vec<TaskId> = {
            let ex = exec.borrow();
            ex.tasks
                .iter()
                .enumerate()
                .filter(|(_, slot)| slot.as_ref().is_some_and(|t| t.scope_id == scope_id))
                .map(|(idx, _)| idx as TaskId)
                .collect()
        };
        let maybe_sched = {
            let mut ex = exec.borrow_mut();
            for tid in task_ids {
                ex.enqueue(tid);
            }
            if ex.in_flush {
                None
            } else {
                ex.try_schedule_flush()
            }
        };
        if let Some(sched) = maybe_sched {
            sched.schedule(Box::new(flush));
        }
    });
}

pub(crate) fn cancel_scope_tasks(scope_id: u64) -> Vec<Pin<Box<dyn Future<Output = ()>>>> {
    EXECUTOR.with(|exec| {
        let mut ex = exec.borrow_mut();
        let mut dropped = Vec::new();

        for slot in &mut ex.tasks {
            if let Some(ref t) = slot {
                if t.scope_id == scope_id {
                    dropped.push(
                        slot.take()
                            .expect("task slot was None after is_some check")
                            .future,
                    );
                }
            }
        }

        // Filter queues.
        let high: Vec<TaskId> = ex
            .high_queue
            .iter()
            .filter(|id| ex.tasks[**id as usize].is_some())
            .copied()
            .collect();
        ex.high_queue.clear();
        ex.high_queue.extend(high);

        let low: Vec<TaskId> = ex
            .low_queue
            .iter()
            .filter(|id| ex.tasks[**id as usize].is_some())
            .copied()
            .collect();
        ex.low_queue.clear();
        ex.low_queue.extend(low);

        let mut all_free: Vec<TaskId> = ex
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_none())
            .map(|(i, _)| i as TaskId)
            .chain(ex.free_slots.iter().copied())
            .collect();
        all_free.sort_unstable();
        all_free.dedup();
        ex.free_slots = all_free;

        dropped
    })
}

// ---------------------------------------------------------------------------
// yield_now
// ---------------------------------------------------------------------------

/// Return a [`Future`] that yields control back to the executor once.
#[must_use = "yield_now() does nothing unless awaited"]
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by [`yield_now`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// schedule_callback — hook for auralis-signal's deferred callback model
// ---------------------------------------------------------------------------

/// Schedule a closure to run at the start of the next executor flush.
///
/// Used internally by `auralis_signal` to defer subscriber callback
/// execution.  The closure is drained before the main poll loop.
pub fn schedule_callback(f: Box<dyn FnOnce()>) {
    EXECUTOR.with(|exec| {
        let maybe_sched = {
            let mut ex = exec.borrow_mut();
            ex.deferred_callbacks.push(f);
            if ex.in_flush {
                None
            } else {
                ex.try_schedule_flush()
            }
        };
        if let Some(sched) = maybe_sched {
            sched.schedule(Box::new(flush));
        }
    });
}

// ---------------------------------------------------------------------------
// set_deferred
// ---------------------------------------------------------------------------

/// Schedule a [`Signal::set`] call for the **next** executor flush.
///
/// Safe to call from inside [`Drop`] — the actual `signal.set(value)` is
/// deferred to a subsequent flush, avoiding re-entrant borrow panics.
pub fn set_deferred<T: 'static>(signal: &Signal<T>, value: T) {
    let signal = signal.clone();
    EXECUTOR.with(|exec| {
        let maybe_sched = {
            let mut ex = exec.borrow_mut();
            ex.deferred_ops.push(DeferredOp {
                f: Box::new(move || signal.set(value)),
            });
            ex.try_schedule_flush()
        };
        if let Some(sched) = maybe_sched {
            sched.schedule(Box::new(flush));
        }
    });
}

// ---------------------------------------------------------------------------
// Test / debug helpers
// ---------------------------------------------------------------------------

/// Completely reset the global executor to a pristine state.
///
/// Clears all task slots, queues, deferred ops, flush/scheduler flags,
/// and injected [`ScheduleFlush`]/[`TimeSource`].  Call at the start
/// of every test to prevent cross-test state leakage.
///
/// # Safety / usage
///
/// This function is intended **only** for testing.  Calling it while
/// the executor is processing tasks will silently drop all live
/// futures and may cause panics or undefined behavior in running
/// application code.
pub fn reset_executor_for_test() {
    PENDING_WAKES.with(|pw| pw.borrow_mut().clear());
    EXECUTOR.with(|exec| {
        let mut ex = exec.borrow_mut();
        ex.high_queue.clear();
        ex.low_queue.clear();
        ex.tasks.clear();
        ex.free_slots.clear();
        ex.next_task_id = 0;
        ex.is_flush_scheduled = false;
        ex.in_flush = false;
        ex.deferred_ops.clear();
        ex.deferred_callbacks.clear();
        ex.flush_scheduler = None;
        ex.time_source = None;
    });
    crate::scope::clear_scope_registry();
}

#[cfg(test)]
pub(crate) fn debug_task_count() -> usize {
    EXECUTOR.with(|exec| exec.borrow().tasks.iter().filter(|t| t.is_some()).count())
}

/// Return a snapshot of all active tasks: `(task_id, priority, scope_id)`.
#[cfg(feature = "debug")]
pub(crate) fn debug_task_snapshot() -> Vec<(TaskId, Priority, u64)> {
    EXECUTOR.with(|exec| {
        let ex = exec.borrow();
        let mut snap = Vec::new();
        for (idx, slot) in ex.tasks.iter().enumerate() {
            if let Some(ref t) = slot {
                snap.push((idx as u64, t.priority, t.scope_id));
            }
        }
        snap
    })
}

/// Return the set of task IDs currently in the ready queues.
#[cfg(feature = "debug")]
pub(crate) fn debug_queued_task_ids() -> Vec<TaskId> {
    EXECUTOR.with(|exec| {
        let ex = exec.borrow();
        let mut ids: Vec<TaskId> = ex
            .high_queue
            .iter()
            .chain(ex.low_queue.iter())
            .copied()
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    })
}

/// Spawn a task without triggering an automatic flush.
/// Used in tests to batch multiple spawns before executing them.
#[cfg(test)]
pub(crate) fn spawn_no_auto_flush(
    priority: Priority,
    future: impl Future<Output = ()> + 'static,
) -> TaskId {
    EXECUTOR.with(|exec| {
        let mut ex = exec.borrow_mut();
        let task_id = ex.allocate_id();
        ex.tasks[task_id as usize] = Some(TaskState {
            future: Box::pin(future),
            priority,
            scope_id: 0,
        });
        ex.enqueue(task_id);
        // Do NOT schedule flush.
        task_id
    })
}

/// Run a manual flush cycle (for tests that need to control timing).
#[cfg(test)]
pub(crate) fn flush_all() {
    flush();
}
