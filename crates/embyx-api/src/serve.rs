//! `serve`: the two listeners, the lifespans around them, signals and the access log.
//!
//! Python origin: `_serve` in `__main__.py` over `uvicorn.run` / `uvicorn.Server` (uvicorn
//! 0.52.1: `server.py`, `lifespan/on.py`, `protocols/http/h11_impl.py`'s access log). The
//! main app listens on `EMBYX_MANAGER_HOST:PORT`; with `EMBYX_MANAGER_TRANSCODE_PORT` the
//! transcode app listens beside it, without lifespan. The lifespans start before either
//! listener binds and stop after both drained. SIGTERM or SIGINT starts the graceful
//! shutdown: no new connections, the requests in flight answered, then the lifespans.
//!
//! Exit codes: 0 after a shutdown (D57), 3 when startup fails (uvicorn's `STARTUP_FAILURE`,
//! also with the transcode listener), 1 for a port past 65535 (`OverflowError`).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{ConnectInfo, Request as AxumRequest};
use embyx_core::pyurl::quote;
use embyx_core::settings::Settings;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

use crate::app::Lifespan;
use crate::bootstrap::Assembled;
use crate::http::Response;
use crate::logging::{ACCESS_TARGET, SERVER_TARGET};
use crate::routing::App;

/// The stack of each runtime worker: the JSON reader recurses once per nesting level, up
/// to Python's limit (`body::MAX_JSON_DEPTH`).
pub const WORKER_STACK_BYTES: usize = 64 << 20;

/// uvicorn's `STARTUP_FAILURE`.
pub const STARTUP_FAILURE: u8 = 3;

/// `http.HTTPStatus(code).phrase` (`CPython` 3.13), for the access log.
pub fn status_phrase(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        102 => "Processing",
        103 => "Early Hints",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        207 => "Multi-Status",
        208 => "Already Reported",
        226 => "IM Used",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        305 => "Use Proxy",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a Teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Content",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        _ => "",
    }
}

/// The access line: `<client> - "<METHOD> <path>[?query] HTTP/<version>" <status> <phrase>`,
/// the path quoted as uvicorn quotes it (`urllib.parse.quote`, `/` kept).
pub fn access_line(
    client: SocketAddr,
    method: &str,
    path: &str,
    query: &str,
    version: http::Version,
    status: u16,
) -> String {
    let version = match version {
        http::Version::HTTP_10 => "1.0",
        http::Version::HTTP_2 => "2",
        _ => "1.1",
    };
    let mut target = quote(path, "/");
    if !query.is_empty() {
        target.push('?');
        target.push_str(query);
    }
    // `"%s:%d" % client`: an IPv6 address unbracketed, as Python formats the tuple.
    format!(
        "{}:{} - \"{method} {target} HTTP/{version}\" {status} {}",
        client.ip(),
        client.port(),
        status_phrase(status)
    )
    .trim_end()
    .to_owned()
}

/// One listener's request handler: the app's answer, then the access line.
async fn answer(app: App, server: SocketAddr, client: SocketAddr, request: AxumRequest) -> Response {
    let method = request.method().as_str().to_owned();
    let version = request.version();
    let path = embyx_core::pyurl::unquote(request.uri().path());
    let query = request.uri().query().unwrap_or("").to_owned();
    let response = app.call(request, Some(server)).await;
    tracing::info!(
        target: ACCESS_TARGET,
        "{}",
        access_line(client, &method, &path, &query, version, response.status().as_u16())
    );
    response
}

fn router(app: App, server: SocketAddr) -> Router {
    Router::new().fallback(
        move |ConnectInfo(client): ConnectInfo<SocketAddr>, request: AxumRequest| {
            let app = app.clone();
            async move { Ok::<_, Infallible>(answer(app, server, client, request).await) }
        },
    )
}

/// `Uvicorn running on http://<host>:<port>`: an IPv6 host in brackets.
fn running_line(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("Uvicorn running on http://[{host}]:{port} (Press CTRL+C to quit)")
    } else {
        format!("Uvicorn running on http://{host}:{port} (Press CTRL+C to quit)")
    }
}

/// Start the lifespans in order; on a failure stop those already started, in reverse.
async fn start_lifespans(lifespans: &[Arc<dyn Lifespan>]) -> Result<(), String> {
    for (index, lifespan) in lifespans.iter().enumerate() {
        if let Err(error) = lifespan.start().await {
            for started in lifespans[..index].iter().rev() {
                if let Err(stop_error) = started.stop().await {
                    tracing::error!(target: SERVER_TARGET, "{}", stop_error.0);
                }
            }
            return Err(error.0);
        }
    }
    Ok(())
}

/// Stop every lifespan in reverse order, each even when one before it failed.
async fn stop_lifespans(lifespans: &[Arc<dyn Lifespan>]) -> bool {
    let mut ok = true;
    for lifespan in lifespans.iter().rev() {
        if let Err(error) = lifespan.stop().await {
            tracing::error!(target: SERVER_TARGET, "{}", error.0);
            ok = false;
        }
    }
    ok
}

/// `lifespan.shutdown()`'s lines around the stops.
async fn shutdown_lifespans(lifespans: &[Arc<dyn Lifespan>]) {
    tracing::info!(target: SERVER_TARGET, "Waiting for application shutdown.");
    if stop_lifespans(lifespans).await {
        tracing::info!(target: SERVER_TARGET, "Application shutdown complete.");
    } else {
        tracing::error!(target: SERVER_TARGET, "Application shutdown failed. Exiting.");
    }
}

/// SIGTERM and SIGINT, one message each, caught from now on (uvicorn installs its
/// handlers before the lifespans start). The first starts the graceful shutdown; a second
/// stops waiting for the requests in flight (`force_exit`).
pub fn install_signals() -> mpsc::UnboundedReceiver<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let (sender, receiver) = mpsc::unbounded_channel();
    for kind in [SignalKind::terminate(), SignalKind::interrupt()] {
        match signal(kind) {
            Ok(mut stream) => {
                let sender = sender.clone();
                tokio::spawn(async move {
                    while stream.recv().await.is_some() {
                        if sender.send(()).is_err() {
                            break;
                        }
                    }
                });
            }
            Err(error) => tracing::error!(target: SERVER_TARGET, "cannot catch a signal: {error}"),
        }
    }
    receiver
}

/// What `serve` runs: the assembled apps, or the reason startup cannot begin.
#[derive(Debug)]
pub enum Startup {
    Ready(Assembled),
    /// The traceback text of a failure that ends startup (`lifespan.startup.failed`).
    Failed(String),
}

/// Bind both listeners: `Err` is the bind failure's text.
async fn bind(host: &str, port: u16) -> Result<TcpListener, String> {
    TcpListener::bind((host, port)).await.map_err(|error| {
        format!(
            "[Errno {}] error while attempting to bind on address ('{host}', {port}): {error}",
            error.raw_os_error().unwrap_or(0)
        )
    })
}

/// `_serve()`: the server until a signal, then its shutdown. Returns the exit code.
pub async fn serve(settings: &Settings, startup: Startup) -> u8 {
    let signals = install_signals();
    serve_until(settings, startup, signals).await
}

/// [`serve`] with the signals given (tests send them themselves): the first message shuts
/// down gracefully, a second one stops waiting for the requests in flight.
pub async fn serve_until(settings: &Settings, startup: Startup, mut signals: mpsc::UnboundedReceiver<()>) -> u8 {
    tracing::info!(target: SERVER_TARGET, "Started server process [{}]", std::process::id());
    tracing::info!(target: SERVER_TARGET, "Waiting for application startup.");
    let assembled = match startup {
        Startup::Ready(assembled) => assembled,
        Startup::Failed(traceback) => {
            tracing::error!(target: SERVER_TARGET, "{traceback}");
            tracing::error!(target: SERVER_TARGET, "Application startup failed. Exiting.");
            return STARTUP_FAILURE;
        }
    };
    if let Err(error) = start_lifespans(&assembled.lifespans).await {
        tracing::error!(target: SERVER_TARGET, "Traceback (most recent call last):\n{error}");
        tracing::error!(target: SERVER_TARGET, "Application startup failed. Exiting.");
        return STARTUP_FAILURE;
    }
    tracing::info!(target: SERVER_TARGET, "Application startup complete.");

    let mut listeners = Vec::new();
    for (app, port) in std::iter::once((assembled.app.clone(), settings.port)).chain(
        settings
            .transcode_port
            .map(|port| (assembled.transcode_app.clone(), port)),
    ) {
        // `socket.bind` raises `OverflowError`, which uvicorn does not catch: a traceback
        // and exit 1 (D57).
        let Ok(port) = u16::try_from(port) else {
            tracing::error!(
                target: SERVER_TARGET,
                "Traceback (most recent call last):\nOverflowError: bind(): port must be 0-65535."
            );
            shutdown_lifespans(&assembled.lifespans).await;
            return 1;
        };
        match bind(&settings.host, port).await {
            Ok(listener) => listeners.push((app, listener, port)),
            Err(error) => {
                tracing::error!(target: SERVER_TARGET, "{error}");
                shutdown_lifespans(&assembled.lifespans).await;
                return STARTUP_FAILURE;
            }
        }
    }

    let (stop, stopped) = watch::channel(false);
    let mut servers = Vec::new();
    for (app, listener, port) in listeners {
        let server = listener
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], port)));
        tracing::info!(target: SERVER_TARGET, "{}", running_line(&settings.host, server.port()));
        let mut stopped = stopped.clone();
        let service = router(app, server).into_make_service_with_connect_info::<SocketAddr>();
        servers.push(tokio::spawn(async move {
            axum::serve(listener, service)
                .with_graceful_shutdown(async move {
                    let _ = stopped.wait_for(|stop| *stop).await;
                })
                .await
        }));
    }
    // A closed channel (no signal source left) also ends the server.
    let _ = signals.recv().await;
    for _ in &servers {
        tracing::info!(target: SERVER_TARGET, "Shutting down");
    }
    let _ = stop.send(true);
    let aborts: Vec<_> = servers.iter().map(tokio::task::JoinHandle::abort_handle).collect();
    let drained = async {
        for server in servers {
            match server.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::error!(target: SERVER_TARGET, "{error}"),
                Err(error) if error.is_cancelled() => {}
                Err(error) => tracing::error!(target: SERVER_TARGET, "{error}"),
            }
        }
    };
    tokio::pin!(drained);
    tokio::select! {
        () = &mut drained => {}
        Some(()) = signals.recv() => {
            // `force_exit`: the requests still in flight are dropped.
            for abort in &aborts {
                abort.abort();
            }
            drained.await;
        }
    }
    shutdown_lifespans(&assembled.lifespans).await;
    tracing::info!(target: SERVER_TARGET, "Finished server process [{}]", std::process::id());
    0
}

/// The whole `serve` command: settings from the environment, the runtime, the server.
pub fn run() -> u8 {
    crate::logging::init();
    let settings = match Settings::from_env() {
        Ok(settings) => settings,
        Err(error) => return traceback_exit(&format!("ValueError: {error}")),
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_BYTES)
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return traceback_exit(&format!("OSError: {error}")),
    };
    runtime.block_on(async {
        let startup = match crate::bootstrap::build_app(&settings, crate::bootstrap::frontend_dist()) {
            Ok(assembled) => Startup::Ready(assembled),
            Err(crate::bootstrap::BuildError::Settings(error)) => {
                return traceback_exit(&format!("ValueError: {error}"));
            }
            Err(crate::bootstrap::BuildError::Database(error)) => {
                Startup::Failed(format!("Traceback (most recent call last):\n{error}"))
            }
        };
        serve(&settings, startup).await
    })
}

/// An exception before the server starts: Python's traceback header and its last line
/// (`<class>: <message>`) on stderr, exit 1 (D141's form).
fn traceback_exit(last_line: &str) -> u8 {
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr().lock(),
        "Traceback (most recent call last):\n{last_line}"
    );
    1
}

#[cfg(test)]
mod tests;
