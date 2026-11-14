//! `serve` on real listeners: both apps answer, the lifespans run around them in order,
//! and a failed startup or bind exits 3 after stopping what started.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use embyx_core::settings::Settings;
use futures_util::future::BoxFuture;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::{STARTUP_FAILURE, Startup, access_line, running_line, serve_until, status_phrase};
use crate::app::Lifespan;
use crate::bootstrap::build_app;
use crate::http::Internal;

/// A lifespan that writes what happens to it into a shared log.
struct Recording {
    name: &'static str,
    fail_start: bool,
    log: Arc<Mutex<Vec<String>>>,
}

impl Lifespan for Recording {
    fn start(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            self.log.lock().expect("log").push(format!("start {}", self.name));
            if self.fail_start {
                Err(Internal::new("RuntimeError: refused"))
            } else {
                Ok(())
            }
        })
    }

    fn stop(&self) -> BoxFuture<'_, Result<(), Internal>> {
        Box::pin(async move {
            self.log.lock().expect("log").push(format!("stop {}", self.name));
            Ok(())
        })
    }
}

/// Settings on loopback with a database nothing listens on; no test here reads it.
fn settings(port: u16, transcode_port: Option<u16>) -> Settings {
    Settings {
        database_url: "postgresql://serve@127.0.0.1:9/serve".to_owned(),
        port: u64::from(port),
        transcode_port: transcode_port.map(u64::from),
        ..Settings::default()
    }
}

/// A port free right now.
async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    listener.local_addr().expect("address").port()
}

fn startup(settings: &Settings, lifespans: &[(&'static str, bool)], log: &Arc<Mutex<Vec<String>>>) -> Startup {
    let mut assembled = build_app(settings, None).expect("assembled");
    assembled.lifespans = lifespans
        .iter()
        .map(|&(name, fail_start)| {
            Arc::new(Recording {
                name,
                fail_start,
                log: Arc::clone(log),
            }) as Arc<dyn Lifespan>
        })
        .collect();
    Startup::Ready(assembled)
}

/// One HTTP/1.1 GET; the raw answer.
async fn get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
    let request = format!("GET {path} HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\nconnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut answer = Vec::new();
    stream.read_to_end(&mut answer).await.expect("read");
    String::from_utf8_lossy(&answer).into_owned()
}

#[tokio::test]
async fn both_listeners_answer_between_the_lifespans() {
    let (port, transcode_port) = (free_port().await, free_port().await);
    let settings = settings(port, Some(transcode_port));
    let log = Arc::new(Mutex::new(Vec::new()));
    let startup = startup(&settings, &[("one", false), ("two", false)], &log);
    let (trigger, signals) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move { serve_until(&settings, startup, signals).await });

    let mut session = String::new();
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok()
            && TcpStream::connect(("127.0.0.1", transcode_port)).await.is_ok()
        {
            session = get(port, "/api/auth/session").await;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(session.starts_with("HTTP/1.1 200 OK\r\n"), "{session}");
    assert!(session.contains("cache-control: no-store"), "{session}");
    assert!(session.ends_with(r#"{"auth_required":false}"#), "{session}");
    // The transcode listener serves its own app: no document, no other routes.
    let transcode = get(transcode_port, "/api/auth/session").await;
    assert!(transcode.starts_with("HTTP/1.1 404 Not Found\r\n"), "{transcode}");
    assert_eq!(*log.lock().expect("log"), ["start one", "start two"]);

    trigger.send(()).expect("the server waits");
    assert_eq!(server.await.expect("the server"), 0);
    assert_eq!(
        *log.lock().expect("log"),
        ["start one", "start two", "stop two", "stop one"]
    );
    assert!(
        TcpStream::connect(("127.0.0.1", port)).await.is_err(),
        "still listening"
    );
}

#[tokio::test]
async fn a_failed_lifespan_stops_those_started_and_exits_3() {
    let settings = settings(free_port().await, None);
    let log = Arc::new(Mutex::new(Vec::new()));
    let startup = startup(&settings, &[("one", false), ("two", true), ("three", false)], &log);
    let (_trigger, signals) = mpsc::unbounded_channel();
    let code = serve_until(&settings, startup, signals).await;
    assert_eq!(code, STARTUP_FAILURE);
    assert_eq!(*log.lock().expect("log"), ["start one", "start two", "stop one"]);
}

#[tokio::test]
async fn a_failed_build_exits_3_before_any_lifespan() {
    let settings = settings(free_port().await, None);
    let (_trigger, signals) = mpsc::unbounded_channel();
    let code = serve_until(&settings, Startup::Failed("Traceback".to_owned()), signals).await;
    assert_eq!(code, STARTUP_FAILURE);
}

#[tokio::test]
async fn a_port_in_use_stops_the_lifespans_and_exits_3() {
    let taken = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let taken_port = taken.local_addr().expect("address").port();
    // The transcode listener cannot bind: uvicorn's `sys.exit(3)`, the main listener closed.
    let settings = settings(free_port().await, Some(taken_port));
    let log = Arc::new(Mutex::new(Vec::new()));
    let startup = startup(&settings, &[("one", false)], &log);
    let (_trigger, signals) = mpsc::unbounded_channel();
    let code = serve_until(&settings, startup, signals).await;
    assert_eq!(code, STARTUP_FAILURE);
    assert_eq!(*log.lock().expect("log"), ["start one", "stop one"]);
    drop(taken);
}

#[test]
fn the_access_line_is_uvicorns() {
    let client: SocketAddr = "127.0.0.1:51000".parse().expect("address");
    assert_eq!(
        access_line(client, "GET", "/api/config/a b", "x=1", http::Version::HTTP_11, 200),
        r#"127.0.0.1:51000 - "GET /api/config/a%20b?x=1 HTTP/1.1" 200 OK"#
    );
    let v6: SocketAddr = "[::1]:8000".parse().expect("address");
    assert_eq!(
        access_line(v6, "PUT", "/api/config/emby", "", http::Version::HTTP_10, 422),
        r#"::1:8000 - "PUT /api/config/emby HTTP/1.0" 422 Unprocessable Content"#
    );
    // An unknown code has no phrase, and the line ends at the number.
    assert_eq!(status_phrase(599), "");
    assert!(access_line(client, "GET", "/", "", http::Version::HTTP_11, 599).ends_with("\" 599"));
}

#[test]
fn the_running_line_brackets_ipv6_hosts() {
    assert_eq!(
        running_line("127.0.0.1", 8000),
        "Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)"
    );
    assert_eq!(
        running_line("::", 8000),
        "Uvicorn running on http://[::]:8000 (Press CTRL+C to quit)"
    );
}

#[tokio::test]
async fn a_port_past_65535_is_python_s_overflow_error() {
    let mut settings = settings(free_port().await, None);
    settings.port = 70_000;
    let log = Arc::new(Mutex::new(Vec::new()));
    let startup = startup(&settings, &[("one", false)], &log);
    let (_trigger, signals) = mpsc::unbounded_channel();
    let code = serve_until(&settings, startup, signals).await;
    assert_eq!(code, 1);
    assert_eq!(*log.lock().expect("log"), ["start one", "stop one"]);
}

/// A second signal stops waiting for a request still in flight; the lifespans still stop.
#[tokio::test]
async fn a_second_signal_drops_the_requests_in_flight() {
    let port = free_port().await;
    let settings = settings(port, None);
    let log = Arc::new(Mutex::new(Vec::new()));
    let startup = startup(&settings, &[("one", false)], &log);
    let (trigger, signals) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move { serve_until(&settings, startup, signals).await });
    // A connection that sends half a request keeps the graceful drain waiting.
    let mut stream = loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)).await {
            break stream;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    stream
        .write_all(b"POST /api/config/emby HTTP/1.1\r\nhost: x\r\ncontent-length: 10\r\n\r\n{")
        .await
        .expect("write");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    trigger.send(()).expect("the server waits");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!server.is_finished(), "the drain waits for the request in flight");
    trigger.send(()).expect("the server waits");
    let code = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("a second signal ends the drain")
        .expect("the server");
    assert_eq!(code, 0);
    assert_eq!(*log.lock().expect("log"), ["start one", "stop one"]);
}
