//! Leptos Todo Dashboard — every signal/memo mirrored into Auralis.

use leptos::prelude::*;

#[derive(Debug, Clone, PartialEq)]
struct TodoItem { id: u64, text: String, done: bool }

#[derive(Debug, Clone, Copy, PartialEq)]
enum Filter { All, Active, Completed }

#[component]
pub fn App() -> impl IntoView {
    let (items, set_items) = signal(Vec::<TodoItem>::new());
    let (filter, set_filter) = signal(Filter::All);
    let (add_count, set_add_count) = signal(0u64);
    let (input_text, set_input_text) = signal(String::new());

    let filtered_items = Memo::new(move |_| match filter.get() {
        Filter::All => items.get(),
        Filter::Active => items.get().into_iter().filter(|t| !t.done).collect(),
        Filter::Completed => items.get().into_iter().filter(|t| t.done).collect(),
    });
    let total_count = Memo::new(move |_| items.get().len());
    let active_count = Memo::new(move |_| items.get().iter().filter(|t| !t.done).count());
    let completed_count = Memo::new(move |_| items.get().iter().filter(|t| t.done).count());
    let completion_rate = Memo::new(move |_| {
        let n = total_count.get();
        if n > 0 { completed_count.get() as f64 / n as f64 } else { 0.0 }
    });

    // ── Auralis Mirrors ─────────────────────────────────────────
    // Base signals.
    let mir_items = crate::mirror!(items, "items");
    let _mir_filter = crate::mirror!(filter, "filter");
    let _mir_add_count = crate::mirror!(add_count, "add_count");

    // Derived memos with deps → edges in the DevTools graph.
    let _mir_total = crate::mirror_memo_deps!(total_count, "total_count", mir_items.clone());
    let _mir_active = crate::mirror_memo_deps!(active_count, "active_count", mir_items.clone());
    let _mir_completed = crate::mirror_memo_deps!(completed_count, "completed_count", mir_items.clone());
    let _mir_rate = crate::mirror_memo!(completion_rate, "completion_rate");

    // ── Handlers ─────────────────────────────────────────────────
    let add_todo = move |_| {
        let text = input_text.get().trim().to_string();
        if !text.is_empty() {
            set_items.update(|v| v.push(TodoItem { id: add_count.get(), text, done: false }));
            set_add_count.update(|v| *v += 1);
            set_input_text.set(String::new());
        }
    };
    let toggle = move |id: u64| {
        set_items.update(move |v| { if let Some(t) = v.iter_mut().find(|t| t.id == id) { t.done = !t.done; } });
    };
    let remove = move |id: u64| {
        set_items.update(move |v| v.retain(|t| t.id != id));
    };
    let filter_btns = [Filter::All, Filter::Active, Filter::Completed].iter().map(|&f| {
        let name = match f { Filter::All => "All", Filter::Active => "Active", Filter::Completed => "Completed" };
        let active = move || filter.get() == f;
        view! {
            <button style:padding="4px 12px" style:border="1px solid #30363d" style:border-radius="4px"
                style:cursor="pointer" style:font-size="12px"
                style:background=move || if active() { "#1f6feb" } else { "#161b22" }
                style:color=move || if active() { "white" } else { "#8b949e" }
                on:click=move |_| set_filter.set(f)>{name}</button>
        }
    }).collect::<Vec<_>>();

    let todo_items = move || filtered_items.get().iter().map(|t| {
        let id = t.id; let text = t.text.clone(); let done = t.done;
        view! {
            <div style:display="flex" style:align-items="center" style:gap="8px"
                style:padding="6px 0" style:border-bottom="1px solid #21262d">
                <input type="checkbox" checked=done on:change=move |_| toggle(id) />
                <span style:flex="1" style:font-size="14px"
                    style:text-decoration=move || if done { "line-through" } else { "none" }
                    style:color=move || if done { "#484f58" } else { "#c9d1d9" }>{text.clone()}</span>
                <button style:background="none" style:border="none" style:color="#f85149"
                    style:cursor="pointer" style:font-size="14px"
                    on:click=move |_| remove(id)>x</button>
            </div>
        }
    }).collect::<Vec<_>>();

    // ── View ─────────────────────────────────────────────────────
    view! {
        <div style:max-width="600px" style:margin="0 auto" style:padding="20px" style:font-family="system-ui">
            <h1 style:color="#58a6ff" style:margin-bottom="16px">Leptos Todo Dashboard</h1>
            <p style:color="#8b949e" style:font-size="12px" style:margin-bottom="20px">
                Every signal/memo is mirrored into Auralis. Open the DevTools panel (bottom-right) to inspect.
            </p>

            <div style:display="flex" style:gap="20px" style:margin-bottom="16px" style:font-size="13px" style:color="#c9d1d9">
                <div>Total: <b>{move || total_count.get()}</b></div>
                <div>Active: <b>{move || active_count.get()}</b></div>
                <div>Completed: <b>{move || completed_count.get()}</b></div>
                <div>Rate: <b>{move || format!("{:.0}%", completion_rate.get() * 100.0)}</b></div>
            </div>

            <div style:display="flex" style:gap="8px" style:margin-bottom="12px">
                <input type="text" placeholder="Add a todo…" prop:value=input_text
                    style:flex="1" style:padding="6px 10px" style:background="#0d1117"
                    style:border="1px solid #30363d" style:color="#c9d1d9" style:border-radius="4px" style:font-size="14px"
                    on:input=move |e| set_input_text.set(event_target_value(&e)) />
                <button style:padding="6px 16px" style:background="#238636" style:color="white" style:border="none"
                    style:border-radius="4px" style:cursor="pointer" style:font-size="14px"
                    on:click=add_todo>Add</button>
            </div>

            <div style:display="flex" style:gap="4px" style:margin-bottom="12px">{filter_btns}</div>
            <div style:margin-bottom="12px">{todo_items}</div>

            <button style:padding="4px 12px" style:background="#161b22" style:border="1px solid #30363d"
                style:color="#8b949e" style:border-radius="4px" style:cursor="pointer" style:font-size="12px"
                on:click=move |_| set_items.update(|v| v.retain(|t| !t.done))
                >Clear completed</button>

            <p style:color="#484f58" style:font-size="11px" style:margin-top="12px">Actions: {move || add_count.get()}</p>
        </div>
    }
}
