//! Explicit [`TaskScope`] tree with iterative cancellation, parent
//! back-references, callback-handle lifecycle management, and a context
//! store for dependency injection.

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::rc::{Rc, Weak};

use auralis_signal::{Memo, Signal};

use crate::executor;
use crate::Priority;

type ScopeId = u64;
type TaskId = u64;

// ---------------------------------------------------------------------------
// Scope-id allocator
// ---------------------------------------------------------------------------

thread_local! {
    static NEXT_SCOPE_ID: Cell<ScopeId> = const { Cell::new(1) };
}

fn alloc_scope_id() -> ScopeId {
    NEXT_SCOPE_ID.with(|c| {
        let id = c.get();
        c.set(id + 1);
        id
    })
}

// ---------------------------------------------------------------------------
// CallbackHandle
// ---------------------------------------------------------------------------

/// Owns a resource that must be cleaned up when the owning [`TaskScope`]
/// is dropped.
///
/// Currently used for signal subscriptions registered by the `bind_*`
/// functions.  When the [`TaskScope`] drops, every registered
/// [`CallbackHandle`] is dropped, which calls the stored cleanup closure
/// to unsubscribe from the signal.
pub struct CallbackHandle {
    cleanup: Option<Box<dyn FnOnce() + 'static>>,
}

impl CallbackHandle {
    /// Create a handle from a cleanup closure.
    pub fn new(cleanup: impl FnOnce() + 'static) -> Self {
        Self {
            cleanup: Some(Box::new(cleanup)),
        }
    }

    /// Create a no-op handle that does nothing on drop.
    ///
    /// Useful as a placeholder when a [`CallbackHandle`] is required
    /// but no cleanup is needed.
    #[must_use]
    pub fn noop() -> Self {
        Self { cleanup: None }
    }
}

impl Drop for CallbackHandle {
    fn drop(&mut self) {
        if let Some(f) = self.cleanup.take() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        }
    }
}

// ---------------------------------------------------------------------------
// Scope registry — maps ScopeId → live TaskScope for executor injection
// ---------------------------------------------------------------------------
//
// # Why Weak references
//
// The registry stores `Weak<RefCell<TaskScopeInner>>` rather than
// `Rc<...>`.  This prevents the registry from keeping scopes alive
// after the application has dropped them — when the last strong
// reference is gone, the Weak upgrade returns `None` and the executor
// skips that scope.
//
// # Thread safety
//
// `SCOPE_REGISTRY` is a `thread_local!` because Auralis is
// single-threaded by design (Wasm constraint).  For multi-task SSR
// servers, each request uses an isolated [`Executor`] instance created
// via [`Executor::new_instance`](crate::Executor::new_instance), and
// the [`ScopeStore`] trait provides pluggable per-task storage.

type ScopeRegistryEntry = (Weak<RefCell<TaskScopeInner>>, Weak<Cell<bool>>);

thread_local! {
    static SCOPE_REGISTRY: RefCell<HashMap<ScopeId, ScopeRegistryEntry>> =
        RefCell::new(HashMap::new());
}

/// Register a scope in the global registry so the executor can look it
/// up by id and inject it as the current scope when polling tasks.
fn register_scope(id: ScopeId, inner: &Rc<RefCell<TaskScopeInner>>, suspended: &Rc<Cell<bool>>) {
    let _ = SCOPE_REGISTRY.try_with(|reg| {
        if let Ok(mut r) = reg.try_borrow_mut() {
            r.insert(id, (Rc::downgrade(inner), Rc::downgrade(suspended)));
        }
    });
}

fn unregister_scope(id: ScopeId) {
    let _ = SCOPE_REGISTRY.try_with(|reg| {
        if let Ok(mut r) = reg.try_borrow_mut() {
            r.remove(&id);
        }
    });
}

/// Find a live [`TaskScope`] by its id.
///
/// Returns `None` if the scope has been dropped or the id is unknown.
#[must_use]
pub fn find_scope(scope_id: ScopeId) -> Option<TaskScope> {
    SCOPE_REGISTRY
        .try_with(|reg| {
            if let Ok(r) = reg.try_borrow() {
                r.get(&scope_id).and_then(|(inner_weak, suspended_weak)| {
                    let inner = inner_weak.upgrade()?;
                    let suspended = suspended_weak.upgrade()?;
                    let cancelled = inner.borrow().cancelled.clone();
                    Some(TaskScope {
                        inner,
                        cancelled,
                        suspended,
                    })
                })
            } else {
                None
            }
        })
        .ok()
        .flatten()
}

/// Return the debug label for the scope with the given id, if any.
///
/// Only available with the `debug` feature.
#[cfg(feature = "debug")]
#[doc(hidden)]
#[must_use]
pub fn scope_debug_label(scope_id: ScopeId) -> Option<String> {
    find_scope(scope_id).and_then(|s| s.inner.borrow().label.clone())
}

/// Clear the scope registry.
#[doc(hidden)]
pub fn clear_scope_registry() {
    let _ = SCOPE_REGISTRY.try_with(|reg| {
        if let Ok(mut r) = reg.try_borrow_mut() {
            r.clear();
        }
    });
}

// ---------------------------------------------------------------------------
// Current-scope storage — injectable, defaults to thread-local
// ---------------------------------------------------------------------------

/// Function signatures for scope store operations.
///
/// Using function pointers keeps the store `Send + Sync` even though
/// `TaskScope` itself is `!Send` — Rust function pointer types are
/// always `Send + Sync` regardless of parameter/return types.
type ScopeSetFn = fn(Option<TaskScope>);
type ScopeGetFn = fn() -> Option<TaskScope>;

/// A pluggable backend for per-task (or per-thread) scope storage.
///
/// The default implementation uses a thread-local cell, which is
/// sufficient for single-threaded Wasm environments.  For multi-task
/// SSR runtimes (e.g. tokio) the host application should inject a
/// task-local implementation via [`set_scope_store`].
pub struct ScopeStore {
    /// Store a scope (or `None` to clear).
    pub set_fn: ScopeSetFn,
    /// Retrieve the current scope.
    pub get_fn: ScopeGetFn,
}

use std::sync::OnceLock;
static SCOPE_STORE: OnceLock<ScopeStore> = OnceLock::new();

fn ensure_default_store() -> &'static ScopeStore {
    SCOPE_STORE.get_or_init(|| ScopeStore {
        set_fn: thread_local_set,
        get_fn: thread_local_get,
    })
}

/// Install a custom scope store.
///
/// Must be called before any scope operations.  On Wasm or in tests the
/// default thread-local store is sufficient.
///
/// # Example (tokio SSR)
///
/// ```rust,ignore
/// use auralis_task::ScopeStore;
///
/// auralis_task::set_scope_store(ScopeStore {
///     set_fn: my_tokio_task_local_set,
///     get_fn: my_tokio_task_local_get,
/// });
/// ```
pub fn set_scope_store(store: ScopeStore) {
    let _ = SCOPE_STORE.set(store);
}

// The `set_scope_store` API allows injecting a custom scope store.
// For SSR in multi-threaded tokio runtimes, users should implement a
// `ScopeStore` backed by `tokio::task::LocalKey` (available when the
// `ssr-tokio` feature is enabled) or a similar per-task mechanism.
//
// Example with tokio (when `ssr-tokio` is enabled):
//
// ```rust,ignore
// use auralis_task::{ScopeStore, set_scope_store};
//
// tokio::task::LocalKey! {
//     static TK_SCOPE: std::cell::RefCell<Option<auralis_task::TaskScope>> =
//         const { std::cell::RefCell::new(None) };
// }
//
// set_scope_store(ScopeStore {
//     set_fn: |s| TK_SCOPE.with(|c| *c.borrow_mut() = s),
//     get_fn: || TK_SCOPE.with(|c| c.borrow().clone()),
// });
// ```
//
// For single-threaded tokio use (LocalSet / spawn_local), the default
// thread-local store works correctly without any configuration.

// ---- default thread-local implementation -------------------------------

thread_local! {
    static CURRENT_SCOPE: RefCell<Option<TaskScope>> = const { RefCell::new(None) };
}

fn thread_local_set(scope: Option<TaskScope>) {
    CURRENT_SCOPE.with(|cell| {
        cell.replace(scope);
    });
}

fn thread_local_get() -> Option<TaskScope> {
    CURRENT_SCOPE.with(|cell| cell.borrow().clone())
}

/// Directly set the current scope without save/restore.
///
/// Used by the executor to inject the owning scope before polling a
/// task.  The caller must restore the previous scope after the poll.
pub(crate) fn set_scope_direct(scope: Option<TaskScope>) {
    let store = ensure_default_store();
    (store.set_fn)(scope);
}

/// Directly get the current scope.
pub(crate) fn get_scope_direct() -> Option<TaskScope> {
    let store = ensure_default_store();
    (store.get_fn)()
}

// ---- ssr-tokio integration ----------------------------------------------

/// Initialise the scope store for tokio-based SSR runtimes.
///
/// Uses `tokio::task::LocalKey` to store the current [`TaskScope`] per
/// tokio task, enabling true multi-request isolation.  Call this once
/// at process startup, before any scope operations.
///
/// Only available with the **`ssr-tokio`** feature (non-wasm).
///
/// # Example
///
/// ```rust,ignore
/// auralis_task::init_scope_store_tokio();
/// ```
#[cfg(feature = "ssr-tokio")]
pub fn init_scope_store_tokio() {
    tokio::task_local! {
        static TK_SCOPE: std::cell::RefCell<Option<TaskScope>>;
    }

    // Initialise the key.
    let _ = TK_SCOPE.try_with(|cell| {
        cell.replace(None);
    });

    set_scope_store(ScopeStore {
        set_fn: |s| {
            let _ = TK_SCOPE.try_with(|cell| {
                cell.replace(s);
            });
        },
        get_fn: || {
            TK_SCOPE
                .try_with(|cell| cell.borrow().clone())
                .ok()
                .flatten()
        },
    });
}

// ---- public API --------------------------------------------------------

/// Set the current [`TaskScope`] for the duration of `f`.
///
/// Set `scope` as the current scope for the duration of `f`,
/// restoring the previous scope afterward.
///
/// Used by framework glue code so that bind functions can discover the
/// owning scope via [`current_scope`].
pub fn with_current_scope<R>(scope: &TaskScope, f: impl FnOnce() -> R) -> R {
    let store = ensure_default_store();
    let prev = (store.get_fn)();
    (store.set_fn)(Some(scope.clone_inner()));
    let result = f();
    (store.set_fn)(prev);
    result
}

/// Get the currently active [`TaskScope`], if any.
#[must_use]
pub fn current_scope() -> Option<TaskScope> {
    let store = ensure_default_store();
    (store.get_fn)()
}

// ---------------------------------------------------------------------------
// TaskScopeInner
// ---------------------------------------------------------------------------

struct TaskScopeInner {
    id: ScopeId,
    task_ids: Vec<TaskId>,
    children: Vec<TaskScope>,
    /// Weak back-reference to parent (set for child scopes).
    parent: Option<Weak<RefCell<TaskScopeInner>>>,
    /// Typed context store for dependency injection.
    context: RefCell<HashMap<TypeId, Rc<dyn Any>>>,
    /// Callback handles registered by bind_* functions.
    callbacks: RefCell<Vec<CallbackHandle>>,
    /// Whether this scope has been cancelled.  Stored as `Rc<Cell<bool>>`
    /// so it can be read/set without borrowing the `RefCell`, avoiding
    /// re-entrant borrow failures during drop.  `TaskScope` holds a clone
    /// of the same `Rc` for direct access.
    cancelled: Rc<Cell<bool>>,
    /// Optional label for `dump_reactive_graph` output.
    label: Option<String>,
    /// The executor that owns tasks spawned in this scope.
    /// Stored as `Rc` (strong reference) so the executor lives
    /// at least as long as the scope — essential for safe
    /// cancellation during drop.
    executor: executor::ExecutorRef,
}

// ---------------------------------------------------------------------------
// JoinHandle — per-task cancellation handle
// ---------------------------------------------------------------------------

/// A handle to a spawned task, allowing individual cancellation.
///
/// Created by [`TaskScope::spawn`] and [`TaskScope::spawn_with_priority`].
/// Dropping the handle does **not** cancel the task — call [`cancel`](JoinHandle::cancel)
/// explicitly, or drop the owning [`TaskScope`] to cancel all tasks at once.
pub struct JoinHandle {
    task_id: Option<TaskId>,
    executor: executor::ExecutorRef,
}

impl JoinHandle {
    /// Cancel this specific task.
    ///
    /// Cancellation drops the task's [`Future`] on the next executor
    /// flush — it is cooperative (`.await`-bound), same as all async
    /// cancellation in Rust.
    ///
    /// No-op if the task has already completed or was spawned into an
    /// already-cancelled scope.
    pub fn cancel(&self) {
        if let Some(tid) = self.task_id {
            executor::cancel_task(&self.executor, tid);
        }
    }

    /// Return `true` if the task has completed (normally or via cancellation).
    ///
    /// Returns `true` for handles created by spawning into an already-cancelled
    /// scope (they never had a real task).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        match self.task_id {
            Some(tid) => executor::is_task_finished(&self.executor, tid),
            None => true,
        }
    }

    /// Return the id of the wrapped task, or `None` if the handle was
    /// created by spawning into an already-cancelled scope.
    #[must_use]
    pub fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }
}

impl fmt::Debug for JoinHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// TaskScope
// ---------------------------------------------------------------------------

/// A node in the scope tree that owns spawned tasks and carries a typed
/// context for dependency injection.
///
/// # Drop guarantee
///
/// When a [`TaskScope`] is dropped, all descendant scopes and their
/// tasks are cancelled **iteratively** using a work queue — recursion
/// is never used, so deeply nested UI trees (200+ levels) never
/// overflow the stack.
///
/// # Cancellation is cooperative
///
/// Cancellation drops the task's [`Future`] and removes it from the
/// executor.  Like all async Rust, this only takes effect at the next
/// `.await` point — a task stuck in a synchronous compute loop cannot
/// be interrupted mid-execution.  This is the same trade-off made by
/// `tokio::task::JoinHandle::abort`.  For long synchronous work,
/// insert [`yield_now`](crate::yield_now) at checkpoints.
///
/// # Context
///
/// Use [`provide`](TaskScope::provide) / [`consume`](TaskScope::consume)
/// for lightweight dependency injection that walks up the scope tree.
///
/// # Callback lifecycle
///
/// [`CallbackHandle`]s registered via
/// [`register_callback_handle`](Self::register_callback_handle) are
/// dropped **before** spawned tasks are cancelled, ensuring that
/// signal subscriptions are removed before any task cleanup.
#[must_use]
pub struct TaskScope {
    inner: Rc<RefCell<TaskScopeInner>>,
    /// Whether this scope has been cancelled (dropped).  Stored outside
    /// the `RefCell` so that [`is_cancelled`](Self::is_cancelled) can be
    /// checked and set without borrowing — avoids re-entrant borrow
    /// panics and ensures the cancelled flag is always set even when
    /// the inner `RefCell` is already borrowed during drop.
    cancelled: Rc<Cell<bool>>,
    /// Whether this scope is suspended.  Stored outside the `RefCell`
    /// for the same reason as `cancelled`.
    suspended: Rc<Cell<bool>>,
}

impl TaskScope {
    /// Create a new root scope on the global thread-local executor.
    ///
    /// For explicit executor ownership use [`TaskScope::with_executor`].
    pub fn new() -> Self {
        Self::with_executor(&executor::current_executor_instance())
    }

    /// Create a new root scope on the given executor.
    ///
    /// All tasks spawned in this scope (and its descendants) run on
    /// `ex`.  The scope holds a strong reference, keeping the executor
    /// alive at least as long as the scope.
    pub fn with_executor(ex: &executor::ExecutorRef) -> Self {
        let cancelled = Rc::new(Cell::new(false));
        let inner = Rc::new(RefCell::new(TaskScopeInner {
            id: alloc_scope_id(),
            task_ids: Vec::new(),
            children: Vec::new(),
            parent: None,
            context: RefCell::new(HashMap::new()),
            callbacks: RefCell::new(Vec::new()),
            cancelled: Rc::clone(&cancelled),
            label: None,
            executor: Rc::clone(ex),
        }));
        let id = inner.borrow().id;
        let suspended = Rc::new(Cell::new(false));
        register_scope(id, &inner, &suspended);
        Self {
            inner,
            cancelled,
            suspended,
        }
    }

    /// Create a child scope that inherits the parent's executor.
    pub fn new_child(parent: &Self) -> Self {
        let ex = parent.inner.borrow().executor.clone();
        let cancelled = Rc::new(Cell::new(false));
        let inner = Rc::new(RefCell::new(TaskScopeInner {
            id: alloc_scope_id(),
            task_ids: Vec::new(),
            children: Vec::new(),
            parent: Some(Rc::downgrade(&parent.inner)),
            context: RefCell::new(HashMap::new()),
            callbacks: RefCell::new(Vec::new()),
            cancelled: Rc::clone(&cancelled),
            label: None,
            executor: ex,
        }));
        let id = inner.borrow().id;
        let suspended = Rc::new(Cell::new(false));
        register_scope(id, &inner, &suspended);
        let child = Self {
            inner,
            cancelled,
            suspended,
        };
        parent.inner.borrow_mut().children.push(child.clone_inner());
        child
    }

    /// Spawn a future in this scope at low priority.
    ///
    /// Returns a [`JoinHandle`] that can cancel this individual task.
    /// Drop the handle to detach (the task keeps running until the
    /// scope is dropped).
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) -> JoinHandle {
        self.spawn_with_priority(Priority::Low, future)
    }

    /// Spawn a future in this scope at the given priority.
    ///
    /// The current scope is set to `self` during the spawn so that any
    /// synchronous work inside the future constructor (e.g. `bind_text`)
    /// can discover the owning scope via [`current_scope`].
    ///
    /// Returns a [`JoinHandle`] that can cancel this individual task.
    pub fn spawn_with_priority(
        &self,
        priority: Priority,
        future: impl Future<Output = ()> + 'static,
    ) -> JoinHandle {
        // Extract fields before spawning so the Ref borrow is released.
        // If the scheduler fires synchronously (e.g. TestScheduleFlush),
        // the spawned task's future is polled immediately, and a nested
        // spawn on the current scope would panic if `inner` were still
        // borrowed.
        let (cancelled, ex, scope_id) = {
            let inner = self.inner.borrow();
            (inner.cancelled.get(), Rc::clone(&inner.executor), inner.id)
        };
        if cancelled {
            return JoinHandle {
                task_id: None,
                executor: ex,
            };
        }
        let task_id = executor::with_executor(&ex, || {
            with_current_scope(self, || {
                executor::spawn_scoped_on(&ex, priority, scope_id, future)
            })
        });
        self.inner.borrow_mut().task_ids.push(task_id);
        JoinHandle {
            task_id: Some(task_id),
            executor: ex,
        }
    }

    /// Spawn a task that calls `f` with the new value whenever `sig` changes.
    ///
    /// This is a convenience wrapper around the common pattern:
    ///
    /// ```ignore
    /// scope.spawn({
    ///     let s = sig.clone();
    ///     async move { loop { s.changed().await; f(&s.read()); } }
    /// });
    /// ```
    ///
    /// Returns a [`JoinHandle`] for individual cancellation.
    pub fn watch<T: Clone + 'static>(
        &self,
        sig: &Signal<T>,
        f: impl FnMut(&T) + 'static,
    ) -> JoinHandle {
        let s = sig.clone();
        let mut f = f;
        self.spawn(async move {
            loop {
                s.changed().await;
                f(&s.read());
            }
        })
    }

    /// Spawn a task that re-runs `effect` whenever any [`Signal`] read
    /// inside it changes — using a [`Memo`](auralis_signal::Memo) internally
    /// to auto-track dependencies.
    ///
    /// The effect is run once immediately to discover its dependencies.
    /// Subsequent runs happen on the executor when a dependency changes.
    ///
    /// Returns a [`JoinHandle`] for individual cancellation.
    pub fn watch_effect(&self, effect: impl Fn() + 'static) -> JoinHandle {
        let memo = Memo::new(effect);
        self.spawn(async move {
            loop {
                memo.changed().await;
                #[allow(clippy::let_unit_value, clippy::ignored_unit_patterns)]
                let _ = memo.read();
            }
        })
    }

    // -- callback lifecycle ------------------------------------------------

    /// Register a [`CallbackHandle`] that will be dropped when this scope
    /// is dropped (or when `clear_callbacks` is called).
    ///
    /// Used by `bind_*` functions to ensure signal subscriptions are
    /// cleaned up when the owning component is destroyed.
    pub fn register_callback_handle(&self, handle: CallbackHandle) {
        let inner = self.inner.borrow();
        if inner.cancelled.get() {
            return;
        }
        inner.callbacks.borrow_mut().push(handle);
    }

    /// Register a cleanup function that runs when this scope is dropped.
    ///
    /// Equivalent to `register_callback_handle(CallbackHandle::new(f))`.
    ///
    /// Cleanup functions run before spawned tasks are cancelled, so they
    /// can safely interact with signals and other resources.
    ///
    /// If the scope is already cancelled, `f` is dropped immediately.
    pub fn on_cleanup(&self, f: impl FnOnce() + 'static) {
        self.register_callback_handle(CallbackHandle::new(f));
    }

    // -- context -----------------------------------------------------------

    /// Store a value of type `T` in this scope.
    ///
    /// The value is wrapped in [`Rc`] so it can be shared.  A subsequent
    /// call to [`consume`](TaskScope::consume) on this scope (or any
    /// descendant) will discover it by walking up the parent chain.
    pub fn provide<T: 'static>(&self, value: T) {
        self.inner
            .borrow()
            .context
            .borrow_mut()
            .insert(TypeId::of::<T>(), Rc::new(value));
    }

    /// Look up a value of type `T` by walking up the scope tree.
    ///
    /// Returns `None` if no ancestor (including `self`) has provided a
    /// value of this type.
    #[must_use]
    pub fn consume<T: 'static>(&self) -> Option<Rc<T>> {
        let mut current = Some(Rc::clone(&self.inner));

        while let Some(inner) = current {
            // Check local context.
            {
                let inner_ref = inner.borrow();
                let ctx = inner_ref.context.borrow();
                if let Some(val) = ctx.get(&TypeId::of::<T>()) {
                    if let Ok(downcast) = val.clone().downcast::<T>() {
                        return Some(downcast);
                    }
                }
            }

            // Walk up to parent.
            let parent = {
                let inner_ref = inner.borrow();
                inner_ref.parent.as_ref().and_then(Weak::upgrade)
            };
            current = parent;
        }

        None
    }

    /// Like [`consume`](TaskScope::consume) but panics if the value is
    /// not found.
    ///
    /// # Panics
    ///
    /// Panics if no ancestor scope has provided a value of type `T`.
    #[must_use]
    #[track_caller]
    pub fn expect_context<T: 'static>(&self) -> Rc<T> {
        self.consume::<T>()
            .unwrap_or_else(|| panic!("context not found: {}", std::any::type_name::<T>()))
    }

    /// Return `true` if this scope has been cancelled (dropped).
    ///
    /// A cancelled scope silently ignores [`spawn`](TaskScope::spawn) calls.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }

    // -- debugging ----------------------------------------------------------

    /// Set a human-readable label for this scope.
    ///
    /// Labels appear in [`dump_reactive_graph`](crate::dump_reactive_graph)
    /// output and are useful for debugging.
    pub fn set_label(&self, label: impl Into<String>) {
        self.inner.borrow_mut().label = Some(label.into());
    }

    /// Return the label set by [`set_label`](Self::set_label), if any.
    #[must_use]
    pub fn label(&self) -> Option<String> {
        self.inner.borrow().label.clone()
    }

    /// Set a label for this scope, shown in [`dump_task_tree`] output.
    ///
    /// Only available with the `debug` feature.
    #[cfg(feature = "debug")]
    #[doc(hidden)]
    #[deprecated(note = "use `set_label` instead")]
    pub fn set_debug_label(&self, label: impl Into<String>) {
        self.set_label(label);
    }

    // -- testing -----------------------------------------------------------

    /// Return the number of spawned tasks in this scope (test-only).
    #[cfg(test)]
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.inner.borrow().task_ids.len()
    }

    /// Return the number of child scopes (test-only).
    #[cfg(test)]
    #[must_use]
    pub fn child_count(&self) -> usize {
        self.inner.borrow().children.len()
    }

    // -- internals ---------------------------------------------------------

    fn clone_inner(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
            cancelled: Rc::clone(&self.cancelled),
            suspended: Rc::clone(&self.suspended),
        }
    }

    /// Run `f` with `self` set as the current scope for the thread.
    ///
    /// Used by framework glue code so bind functions can discover the
    /// owning scope via [`current_scope`].
    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        with_current_scope(self, f)
    }

    /// Suspend all tasks owned by this scope and its descendants.
    ///
    /// Suspended tasks are skipped during executor polling.  Signal
    /// subscriptions remain registered but their callbacks are not
    /// invoked while the scope is suspended.  Use [`resume`](Self::resume)
    /// to restart execution.
    ///
    /// Used by `if_async_cached` and `match_async_cached` to pause
    /// hidden branches.
    pub fn suspend(&self) {
        if self.suspended.get() {
            return;
        }
        self.suspended.set(true);
        // Cascading: suspend all descendants.
        let children: Vec<TaskScope> = {
            self.inner
                .borrow()
                .children
                .iter()
                .map(TaskScope::clone_inner)
                .collect()
        };
        for child in &children {
            child.suspend();
        }
    }

    /// Resume all tasks owned by this scope and its descendants.
    ///
    /// This reverses the effect of [`suspend`](Self::suspend).  Tasks
    /// become eligible for polling again on the next executor flush.
    pub fn resume(&self) {
        if !self.suspended.get() {
            return;
        }
        self.suspended.set(false);

        let (task_ids, children) = {
            let inner = self.inner.borrow();
            let tids = inner.task_ids.clone();
            let children: Vec<TaskScope> =
                inner.children.iter().map(TaskScope::clone_inner).collect();
            (tids, children)
        };

        // Enqueue all tasks belonging to this scope.
        let ex = Rc::clone(&self.inner.borrow().executor);
        executor::enqueue_scope_tasks_on(&ex, &task_ids);

        // Resume children (cascading).
        for child in &children {
            child.resume();
        }
    }

    /// Return `true` if this scope is currently suspended.
    #[must_use]
    pub fn is_suspended(&self) -> bool {
        self.suspended.get()
    }
}

impl Default for TaskScope {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TaskScope {
    fn clone(&self) -> Self {
        self.clone_inner()
    }
}

// Iterative cancellation: descendants are collected BFS, then
// cancelled leaf→root, avoiding recursive drop that would overflow
// the stack on deeply-nested trees (200+ levels).
//
// Callback handles are dropped BEFORE tasks, ensuring signal
// subscriptions are removed before any task is cancelled.
impl Drop for TaskScope {
    fn drop(&mut self) {
        // Only cancel when this is the last reference to the inner.
        // Temporary clones (from find_scope during executor flush,
        // from with_current_scope during spawn) share the same inner
        // and must not cancel the scope when they go out of scope.
        if Rc::strong_count(&self.inner) > 1 {
            return;
        }

        // Always set cancelled first — this Cell is outside the RefCell
        // and always writable, so the scope is marked cancelled even if
        // we can't do full cleanup below.
        self.cancelled.set(true);

        let Ok(mut inner) = self.inner.try_borrow_mut() else {
            // Already borrowed — re-entrant drop (e.g. a callback or
            // spawned task dropped the last clone during executor flush).
            // Cancelled flag is set, so future spawns are rejected and
            // the executor will clean up tasks on the next flush.
            eprintln!(
                "[auralis-task] WARNING: TaskScope::drop cannot borrow inner \
                 (already borrowed). Tasks and callbacks in this scope will \
                 be cleaned up on the next executor flush. Avoid dropping \
                 the last TaskScope clone inside a callback."
            );
            return;
        };

        // ---- drop callback handles first ---------------------------------
        inner.callbacks.borrow_mut().clear();

        // ---- collect descendants BFS ------------------------------------
        let mut descendants: Vec<Rc<RefCell<TaskScopeInner>>> = Vec::new();
        {
            let mut queue: VecDeque<Rc<RefCell<TaskScopeInner>>> = VecDeque::new();
            for child in &inner.children {
                queue.push_back(Rc::clone(&child.inner));
            }

            while let Some(scope_rc) = queue.pop_front() {
                let scope = scope_rc.borrow();
                for child in &scope.children {
                    queue.push_back(Rc::clone(&child.inner));
                }
                descendants.push(Rc::clone(&scope_rc));
            }
        }

        // ---- cancel leaves → root ---------------------------------------
        for scope_rc in descendants.iter().rev() {
            let mut scope = scope_rc.borrow_mut();
            if scope.cancelled.get() {
                continue;
            }
            scope.cancelled.set(true);

            // Drop callbacks before tasks.
            scope.callbacks.borrow_mut().clear();

            if !scope.task_ids.is_empty() {
                let ex = Rc::clone(&scope.executor);
                let task_ids = std::mem::take(&mut scope.task_ids);
                let dropped_futures = executor::cancel_scope_tasks_on(&ex, &task_ids);
                drop(dropped_futures);
            }
            scope.context.borrow_mut().clear();
            unregister_scope(scope.id);
        }

        // ---- cancel own tasks -------------------------------------------
        if !inner.task_ids.is_empty() {
            let ex = Rc::clone(&inner.executor);
            let task_ids = std::mem::take(&mut inner.task_ids);
            let dropped_futures = executor::cancel_scope_tasks_on(&ex, &task_ids);
            drop(dropped_futures);
        }

        inner.context.borrow_mut().clear();
        inner.children.clear();

        // Remove from the global registry so stale lookups return None.
        unregister_scope(inner.id);
    }
}

// ---------------------------------------------------------------------------
// Convenience macros for context injection / retrieval
// ---------------------------------------------------------------------------

/// Shorthand for `scope.provide(value)`.
///
/// ```rust,ignore
/// provide_context!(scope, 42i32);
/// ```
#[macro_export]
macro_rules! provide_context {
    ($scope:expr, $value:expr) => {
        $scope.provide($value)
    };
}

/// Shorthand for `scope.consume::<T>()`.
///
/// ```rust,ignore
/// let theme: Option<Rc<Theme>> = consume_context!(scope, Theme);
/// ```
#[macro_export]
macro_rules! consume_context {
    ($scope:expr, $ty:ty) => {
        $scope.consume::<$ty>()
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::items_after_statements)]
#[path = "scope_tests.rs"]
mod tests;
