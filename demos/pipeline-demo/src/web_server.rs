//! WebSocket server that pushes real-time DevTools snapshots to a
//! browser frontend.
//!
//! Start the pipeline, then open `demos/pipeline-demo/web/index.html`
//! in a browser.  The page connects to `ws://127.0.0.1:9642` and
//! renders live signal/memo/task data.

use std::net::TcpListener;
use std::time::Duration;

use auralis_devtools::stream::change_stream;
use tungstenite::accept;

/// Start the WebSocket server on `ws://127.0.0.1:9642`.
///
/// Serves one client at a time; reconnects are accepted after
/// disconnect.
pub fn serve() {
    let addr = "127.0.0.1:9642";
    let listener = TcpListener::bind(addr).expect("bind");
    eprintln!("  WebSocket server → ws://{addr}");
    eprintln!("  Open demos/pipeline-demo/web/index.html in a browser\n");

    #[allow(clippy::never_loop)]
    for stream in listener.incoming() {
        let mut ws = accept(stream.expect("tcp accept")).expect("ws handshake");
        eprintln!("  client connected");

        send_snapshot(&mut ws);

        let rx = change_stream();
        let mut last = rx.current_seq();
        loop {
            let _ = rx.wait_timeout(Duration::from_millis(200));
            let current = rx.current_seq();
            if current != last {
                send_snapshot(&mut ws);
                last = current;
            }
        }
    }
}

fn send_snapshot<S: std::io::Read + std::io::Write>(ws: &mut tungstenite::WebSocket<S>) {
    let snap = auralis_devtools::snapshot();
    if let Ok(json) = serde_json::to_string(&snap) {
        let _ = ws.send(tungstenite::Message::Text(json.into()));
    }
}
