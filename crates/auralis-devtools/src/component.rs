//! Framework-agnostic component-tree registry.
//!
//! Any framework (Leptos, Dioxus, Sycamore, Yew) that renders
//! components synchronously can use this: wrap each component fn
//! body in [`ComponentGuard::enter(name)`](ComponentGuard::enter).
//! Signals created via `mirror!` during that scope are automatically
//! associated with the component.
//!
//! # How it works
//!
//! ```text
//! parent component fn
//!   ├─ ComponentGuard::enter("Parent")  → push onto stack
//!   ├─ mirror!(sig_a, "sig_a")          → attach_signal() to Parent
//!   ├─ child component fn
//!   │   ├─ ComponentGuard::enter("Child") → push onto stack
//!   │   ├─ mirror!(sig_b, "sig_b")        → attach_signal() to Child
//!   │   └─ ComponentGuard drop             → pop stack
//!   └─ ComponentGuard drop                → pop stack
//! ```
//!
//! If no component guard is active, signals go to an "Unattached"
//! virtual node — visible in the snapshot but not under any component.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use serde::Serialize;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ComponentId = u64;
type SignalAddr = usize;

/// A registered component node.
struct ComponentRecord {
    label: String,
    parent_id: Option<ComponentId>,
    signal_addrs: Vec<SignalAddr>,
}

thread_local! {
    static NEXT_ID: Cell<ComponentId> = const { Cell::new(0) };
    static COMPONENT_STACK: RefCell<Vec<ComponentId>> = const { RefCell::new(Vec::new()) };
    static COMPONENT_REGISTRY: RefCell<HashMap<ComponentId, ComponentRecord>> =
        RefCell::new(HashMap::new());
}

fn alloc_id() -> ComponentId {
    NEXT_ID.with(|c| {
        let id = c.get();
        c.set(id + 1);
        id
    })
}

// ---------------------------------------------------------------------------
// ComponentGuard
// ---------------------------------------------------------------------------

/// RAII guard that registers a component and pushes it onto the
/// call stack.  Signals created while this guard is active are
/// automatically associated with the component.
///
/// Drop the guard (or let it go out of scope) to pop the stack.
///
/// # Example
///
/// ```rust,ignore
/// #[component]
/// fn TodoList() -> impl IntoView {
///     let _guard = auralis_devtools::component::ComponentGuard::enter("TodoList");
///     // ... mirror! calls here are automatically attached to "TodoList"
/// }
/// ```
pub struct ComponentGuard {
    _id: ComponentId,
}

impl ComponentGuard {
    /// Register a new component with the given label and push it
    /// onto the stack.  Returns a guard that pops the stack on drop.
    #[must_use]
    pub fn enter(label: &str) -> Self {
        let parent_id = COMPONENT_STACK.with(|s| s.borrow().last().copied());
        let id = alloc_id();
        COMPONENT_REGISTRY.with(|reg| {
            reg.borrow_mut().insert(
                id,
                ComponentRecord {
                    label: label.to_string(),
                    parent_id,
                    signal_addrs: Vec::new(),
                },
            );
        });
        COMPONENT_STACK.with(|s| s.borrow_mut().push(id));
        Self { _id: id }
    }
}

impl Drop for ComponentGuard {
    fn drop(&mut self) {
        COMPONENT_STACK.with(|s| s.borrow_mut().pop());
    }
}

// ---------------------------------------------------------------------------
// Signal attachment — called by mirror! macros
// ---------------------------------------------------------------------------

/// Attach a signal address to the current component (the top of
/// the call stack).  No-op if no component guard is active.
pub fn attach_signal(addr: usize) {
    COMPONENT_STACK.with(|s| {
        if let Some(&comp_id) = s.borrow().last() {
            COMPONENT_REGISTRY.with(|reg| {
                if let Some(rec) = reg.borrow_mut().get_mut(&comp_id) {
                    rec.signal_addrs.push(addr);
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------
// Tree export
// ---------------------------------------------------------------------------

/// A node in the component tree, serializable for `DevTools`.
#[derive(Debug, Clone, Serialize)]
pub struct ComponentTreeNode {
    /// Label set via `ComponentGuard::enter(label)`.
    pub label: String,
    /// Hex addresses of signals owned by this component.
    pub signal_addrs: Vec<String>,
    /// Child components (recursive).
    pub children: Vec<ComponentTreeNode>,
}

fn fmt_addr(addr: usize) -> String {
    format!("{addr:#x}")
}

fn build_node(
    id: ComponentId,
    reg: &HashMap<ComponentId, ComponentRecord>,
    children: &HashMap<ComponentId, Vec<ComponentId>>,
) -> ComponentTreeNode {
    let rec = reg.get(&id).expect("component record must exist");
    ComponentTreeNode {
        label: rec.label.clone(),
        signal_addrs: rec.signal_addrs.iter().map(|a| fmt_addr(*a)).collect(),
        children: children
            .get(&id)
            .map(|kids| {
                let mut sorted = kids.clone();
                sorted.sort_unstable();
                sorted
                    .iter()
                    .map(|&cid| build_node(cid, reg, children))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Build the component tree from the registry.
#[must_use]
pub fn component_tree() -> Vec<ComponentTreeNode> {
    COMPONENT_REGISTRY.with(|reg| {
        let r = reg.borrow();
        let mut roots: Vec<ComponentId> = Vec::new();
        let mut children: HashMap<ComponentId, Vec<ComponentId>> = HashMap::new();

        for (&id, rec) in r.iter() {
            match rec.parent_id {
                Some(parent) => children.entry(parent).or_default().push(id),
                None => roots.push(id),
            }
        }

        let mut tree = Vec::new();
        roots.sort_unstable();
        for rid in &roots {
            tree.push(build_node(*rid, &r, &children));
        }
        tree
    })
}
