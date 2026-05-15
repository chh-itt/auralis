//! CLI for auralis-devtools.
//!
//! ```text
//! auralis-devtools dump     Print a JSON snapshot to stdout
//! auralis-devtools stream   Print ChangeEvents to stdout (pipe-friendly)
//! auralis-devtools serve    Start a WebSocket server (requires ws-transport feature)
//! ```

use std::env;
use std::io::{self, Write};
use std::time::Duration;

fn main() {
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("dump");

    match cmd {
        "dump" => cmd_dump(),
        "stream" => cmd_stream(),
        "serve" => cmd_serve(),
        _ => {
            eprintln!("usage: auralis-devtools [dump|stream|serve]");
            std::process::exit(1);
        }
    }
}

fn cmd_dump() {
    let snap = auralis_devtools::snapshot();
    let json = serde_json::to_string_pretty(&snap).expect("serialization should not fail");
    println!("{json}");
}

fn cmd_stream() {
    let rx = auralis_devtools::stream::change_stream();
    let mut last = rx.current_seq();
    loop {
        // Block until a signal changes, or heartbeat every 500 ms.
        let _ = rx.wait_timeout(Duration::from_millis(500));
        let current = rx.current_seq();
        if current != last {
                        let event = auralis_devtools::stream::ChangeEvent { seq: current, addr: 0, version: 0, ms_since_start: 0 };
            let line = serde_json::to_string(&event).expect("serialization should not fail");
            println!("{line}");
            let _ = io::stdout().flush();
            last = current;
        }
    }
}

#[cfg(not(feature = "ws-transport"))]
fn cmd_serve() {
    eprintln!(
        "The 'serve' command requires the 'ws-transport' feature.\n\
         Rebuild with: cargo run --features ws-transport -- serve"
    );
    std::process::exit(1);
}

#[cfg(feature = "ws-transport")]
fn cmd_serve() {
    use std::net::TcpListener;
    use tungstenite::accept;

    let addr = "127.0.0.1:9642";
    let listener = TcpListener::bind(addr).expect("bind");
    eprintln!("auralis-devtools WebSocket server listening on ws://{addr}");

    for stream in listener.incoming() {
        let mut ws = accept(stream.expect("tcp accept")).expect("ws handshake");
        eprintln!("client connected");

        // Send initial snapshot.
        let snap = auralis_devtools::snapshot();
        let json = serde_json::to_string(&snap).expect("serialization");
        ws.send(tungstenite::Message::Text(json.into())).ok();

        // Stream changes.
        let rx = auralis_devtools::stream::change_stream();
        let mut last = rx.current_seq();
        loop {
            let _ = rx.wait_timeout(Duration::from_millis(200));
            let current = rx.current_seq();
            if current != last {
                            let event = auralis_devtools::stream::ChangeEvent { seq: current, addr: 0, version: 0, ms_since_start: 0 };
                let line = serde_json::to_string(&event).expect("serialization");
                if ws.send(tungstenite::Message::Text(line.into())).is_err() {
                    break; // client disconnected
                }
                last = current;
            }
        }
        eprintln!("client disconnected");
    }
}
