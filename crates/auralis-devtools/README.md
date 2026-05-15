# auralis-devtools

**Diagnostic DevTools for the Auralis reactive kernel.**

[![CI](https://github.com/chh-itt/auralis/actions/workflows/ci.yml/badge.svg)](https://github.com/chh-itt/auralis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/chh-itt/auralis/blob/main/LICENSE)

Built on `auralis-signal` (diagnostics feature) and `auralis-task` (debug feature).
`#![forbid(unsafe_code)]`. Zero dependencies in the core; optional `tungstenite` for
WebSocket transport.

## Overview

| API | Role |
|-----|------|
| `snapshot()` | Take a JSON-serializable snapshot of every live signal, memo, and task |
| `change_stream()` | Real-time event stream — an observer fires on every signal mutation, delivered through an `mpsc` channel |
| `ChangeReceiver::wait_timeout(d)` | Block until the next change (or timeout) without busy-polling |

### CLI

```text
auralis-devtools dump       Print a JSON snapshot to stdout
auralis-devtools stream     Print ChangeEvents to stdout (pipe-friendly)
auralis-devtools serve      WebSocket server on ws://127.0.0.1:9642 (requires ws-transport)
```

## Quick Start

```rust
use auralis_devtools::snapshot;

let snap = snapshot();
println!("{}", serde_json::to_string_pretty(&snap).unwrap());
// → {"signals": [...], "memos": [...], "task_tree": "..."}
```

## Snapshot JSON schema

```json
{
  "signals": [
    {
      "label": "counter",
      "version": 42,
      "subscriber_count": 3,
      "addr": "0x..."
    }
  ],
  "memos": [
    {
      "label": "sum",
      "version": 42,
      "subscriber_count": 1,
      "is_dirty": false,
      "compute_count": 15,
      "dependency_count": 2,
      "dependency_addrs": ["0x...", "0x..."],
      "addr": "0x..."
    }
  ],
  "task_tree": "=== Auralis Reactive Graph ===\n..."
}
```

The `dependency_addrs` field maps each memo to its source signals, enabling
frontend tools to render a dependency graph (arrows from source signals to
the memo).

## Feature Flags

| Feature | Enables |
|---------|---------|
| `ws-transport` | `auralis-devtools serve` command (adds `tungstenite` dependency) |

## License

Licensed under either of [MIT](https://github.com/chh-itt/auralis/blob/main/LICENSE) or Apache 2.0 at your option.
