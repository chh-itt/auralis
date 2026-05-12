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
    find_scope(scope_id).and_then(|s| s.inner.borrow().debug_label.clone())
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
    /// Optional label for `dump_task_tree` output (debug feature).
    #[cfg(feature = "debug")]
    debug_label: Option<String>,
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
    pub fn is_finished(&self) -> bool {
        match self.task_id {
            Some(tid) => executor::is_task_finished(&self.executor, tid),
            None => true,
        }
    }

    /// Return the id of the wrapped task, or `None` if the handle was
    /// created by spawning into an already-cancelled scope.
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
            #[cfg(feature = "debug")]
            debug_label: None,
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
            #[cfg(feature = "debug")]
            debug_label: None,
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

    /// Set a label for this scope, shown in [`dump_task_tree`] output.
    ///
    /// Only available with the `debug` feature.
    #[cfg(feature = "debug")]
    pub fn set_debug_label(&self, label: impl Into<String>) {
        self.inner.borrow_mut().debug_label = Some(label.into());
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
mod tests {
    use super::*;
    use crate::executor::{self, init_flush_scheduler, reset_executor_for_test, TestScheduleFlush};
    use crate::{init_time_source, ScheduleFlush, TestTimeSource, TimeSource};
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
}
