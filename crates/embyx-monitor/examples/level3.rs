//! Level 3, the Rust side: one capped, anonymous, read-only live run of the site clients
//! (docs/rewrite/PLAN.md, WP16). `tools/live/level3.py` holds the Python control, the list of
//! reads and the comparison; this side sends the same reads through the Rust clients and
//! writes the same JSON shape.
//!
//!     cargo run -q -p embyx-monitor --example level3 -- --out rust.json [--gap 2.5] [--roots BUNDLE.pem]
//!
//! At least `--gap` seconds between two calls, GETs only, no credentials, and no retry loop:
//! the JavBus and sukebei clients' retry sleeper ends the call on its first wait instead of
//! sleeping (`tenacity` with one attempt), so a refusal is recorded once. `--roots` trusts the
//! certificates of a PEM bundle instead of the bundled roots (a TLS-inspecting egress proxy);
//! the proxy comes from `HTTPS_PROXY`, which every client reads. Never runs in CI.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use embyx_clients::avbase::session::ImpersonatingSession;
use embyx_clients::avbase::{AvbaseClient, AvbaseOptions};
use embyx_clients::feeds::HttpFeedFetcher;
use embyx_clients::javbus::{JavBusClient, JavBusOptions};
use embyx_clients::jinjier::{self, JinjierClient};
use embyx_clients::net::ClientOptions;
use embyx_clients::net::retry::Sleeper;
use embyx_clients::sukebei::{SukebeiClient, SukebeiOptions};
use embyx_core::avid::{AvidConfig, AvidParser};
use serde_json::{Value, json};
use tokio::sync::Notify;

const STAR_NAME: &str = "三上悠亜";
const VIDEO_ID: &str = "SSIS-001";
const SUKEBEI_QUERY: &str = "SSIS-001";
const AVBASE_TALENT: &str = "三上悠亜";
const AVBASE_WORK: &str = "SSIS-001";
const RSSHUB_FEED: &str = "https://rsshub.app/github/issue/DIYgod/RSSHub";
const JINJIER_SOURCE: &str = "https://jinjier.art/sql";

struct Pacer {
    gap: Duration,
    last: Option<Instant>,
}

impl Pacer {
    async fn wait(&mut self) {
        if let Some(last) = self.last {
            let due = last + self.gap;
            let now = Instant::now();
            if due > now {
                tokio::time::sleep(due - now).await;
            }
        }
        self.last = Some(Instant::now());
    }
}

/// The retry sleeper that ends the call instead of waiting: it wakes `refused` and never
/// finishes, and [`Run::call`] stops the call when `refused` fires.
fn one_attempt(refused: Arc<Notify>) -> Sleeper {
    Arc::new(move |_seconds| {
        let refused = Arc::clone(&refused);
        Box::pin(async move {
            refused.notify_one();
            std::future::pending::<()>().await;
        }) as Pin<Box<dyn Future<Output = ()> + Send>>
    })
}

struct Run {
    pacer: Pacer,
    refused: Arc<Notify>,
    results: Vec<Value>,
}

impl Run {
    async fn call<T, E: std::fmt::Display>(
        &mut self,
        step: &str,
        call: impl Future<Output = Result<T, E>>,
        class: impl Fn(&E) -> String,
        summary: impl Fn(&T) -> Value,
    ) -> Option<T> {
        self.pacer.wait().await;
        let started = Instant::now();
        let outcome = tokio::select! {
            outcome = call => Some(outcome),
            () = self.refused.notified() => None,
        };
        match outcome {
            Some(Ok(value)) => {
                let seconds = (started.elapsed().as_secs_f64() * 100.0).round() / 100.0;
                self.results
                    .push(json!({"step": step, "seconds": seconds, "value": summary(&value)}));
                Some(value)
            }
            Some(Err(error)) => {
                let message: String = error.to_string().chars().take(300).collect();
                self.results
                    .push(json!({"step": step, "error": {"class": class(&error), "message": message}}));
                None
            }
            None => {
                self.results.push(json!({"step": step, "error": {
                    "class": "RetryableError",
                    "message": "the first attempt failed with a retryable error; no retry (one attempt)",
                }}));
                None
            }
        }
    }

    fn push(&mut self, step: &str, value: &Value) {
        self.results.push(json!({"step": step, "value": value}));
    }
}

fn int(value: i128) -> Value {
    i64::try_from(value).map_or_else(|_| Value::String(value.to_string()), Value::from)
}

fn debug_class<E: std::fmt::Debug>(error: &E) -> String {
    let text = format!("{error:?}");
    text.split(['(', ' ', '{']).next().unwrap_or_default().to_owned()
}

/// The certificates of a PEM bundle, as DER.
fn pem_roots(path: &PathBuf) -> Result<Vec<Vec<u8>>, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let mut roots = Vec::new();
    let mut body: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line == "-----BEGIN CERTIFICATE-----" {
            body = Some(String::new());
        } else if line == "-----END CERTIFICATE-----" {
            let encoded = body.take().unwrap_or_default();
            roots.push(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|error| error.to_string())?,
            );
        } else if let Some(body) = body.as_mut() {
            body.push_str(line);
        }
    }
    Ok(roots)
}

struct Args {
    out: PathBuf,
    gap: f64,
    roots: Vec<Vec<u8>>,
}

fn parse_args() -> Result<Args, String> {
    let mut out = None;
    let mut gap = 2.5;
    let mut roots = Vec::new();
    let mut rest = std::env::args().skip(1);
    while let Some(argument) = rest.next() {
        let mut value = || rest.next().ok_or_else(|| format!("{argument} needs a value"));
        match argument.as_str() {
            "--out" => out = Some(PathBuf::from(value()?)),
            "--gap" => gap = value()?.parse().map_err(|_| "--gap: not a number")?,
            "--roots" => roots = pem_roots(&PathBuf::from(value()?))?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Args {
        out: out.ok_or("give --out FILE")?,
        gap,
        roots,
    })
}

#[allow(clippy::too_many_lines, reason = "one fixed list of reads, in order")]
async fn run(args: &Args) -> Result<Vec<Value>, String> {
    let refused = Arc::new(Notify::new());
    let mut run = Run {
        pacer: Pacer {
            gap: Duration::from_secs_f64(args.gap),
            last: None,
        },
        refused: Arc::clone(&refused),
        results: Vec::new(),
    };
    let with_roots = |mut options: ClientOptions| {
        options.roots.clone_from(&args.roots);
        options
    };

    let javbus_options = JavBusOptions::default();
    let javbus = JavBusClient::with_client(
        &javbus_options,
        &with_roots(JavBusClient::client_options(&javbus_options)),
        one_attempt(Arc::clone(&refused)),
    )
    .map_err(|error| error.to_string())?;
    let sukebei_options = SukebeiOptions::default();
    let sukebei = SukebeiClient::with_client(
        &sukebei_options,
        &with_roots(SukebeiClient::client_options(&sukebei_options)),
        one_attempt(Arc::clone(&refused)),
    )
    .map_err(|error| error.to_string())?;
    let avbase_options = AvbaseOptions::default();
    let mut session_options = AvbaseClient::session_options(&avbase_options);
    session_options.roots.clone_from(&args.roots);
    // wreq reads HTTPS_PROXY itself; named here as well so the run states its egress.
    session_options.proxy = std::env::var("HTTPS_PROXY").ok().filter(|proxy| !proxy.is_empty());
    let avbase = AvbaseClient::with_session(&avbase_options, Arc::new(ImpersonatingSession::new(&session_options)?));
    let jinjier = JinjierClient::with_client(
        JINJIER_SOURCE,
        &with_roots(JinjierClient::client_options(Duration::from_mins(2))),
    )
    .map_err(|error| format!("{error:?}"))?;
    let feeds = HttpFeedFetcher::with_client(
        &with_roots(HttpFeedFetcher::client_options(Duration::from_mins(2), None)),
        embyx_clients::feeds::MAX_FEED_BYTES,
    )
    .map_err(|error| error.to_string())?;

    let stars = run
        .call(
            "javbus.search_stars",
            javbus.search_stars(STAR_NAME),
            debug_class,
            |stars| {
                Value::Array(
                    stars
                        .iter()
                        .map(|star| json!({"actor_id": star.actor_id, "name": star.name}))
                        .collect(),
                )
            },
        )
        .await;
    if let Some(first) = stars.as_ref().and_then(|stars| stars.first()) {
        run.call(
            "javbus.get_actor",
            javbus.get_actor(&first.actor_id),
            debug_class,
            |page| {
                page.as_ref().map_or(
                    Value::Null,
                    |page| json!({"actor_id": page.actor_id, "name": page.name, "video_ids": page.video_ids}),
                )
            },
        )
        .await;
    }
    run.call(
        "javbus.get_magnets",
        javbus.get_magnets(VIDEO_ID),
        debug_class,
        |magnets| {
            Value::Array(
                magnets
                    .iter()
                    .map(|magnet| {
                        json!({"magnet": magnet.magnet, "size": magnet.size, "size_int": int(magnet.size_int),
                           "tags": magnet.tags, "score": int(magnet.score)})
                    })
                    .collect(),
            )
        },
    )
    .await;
    run.call(
        "sukebei.search",
        sukebei.search(SUKEBEI_QUERY, 2, 2, 0, 0),
        debug_class,
        |items| {
            Value::Array(
                items
                    .iter()
                    .map(|item| {
                        json!({"id": item.id, "category": item.category, "url": item.url, "name": item.name,
                           "download_url": item.download_url, "magnet": item.magnet, "size": item.size,
                           "date": item.date, "seeders": item.seeders, "leechers": item.leechers,
                           "completed_downloads": item.completed_downloads, "type": item.kind})
                    })
                    .collect(),
            )
        },
    )
    .await;
    run.call("avbase.build_id", avbase.build_id(false), debug_class, |id| json!(id))
        .await;
    run.call("avbase.talent", avbase.talent(AVBASE_TALENT), debug_class, |talent| {
        talent.as_ref().map_or(Value::Null, |talent| {
            json!({"talent_id": int(talent.talent_id), "name": talent.name, "aliases": talent.aliases,
                   "total_works": int(talent.total_works)})
        })
    })
    .await;
    run.call("avbase.work", avbase.work(AVBASE_WORK), debug_class, |work| {
        work.as_ref().map_or(Value::Null, |work| {
            json!({"work_id": work.work_id, "prefix": work.prefix, "title": work.title,
                   "release_date": work.release_date.map(|date| date.to_string()),
                   "cast": work.cast.iter().map(|member| json!({"actor_id": int(member.actor_id),
                       "name": member.name, "talent_id": member.talent_id.map(int)})).collect::<Vec<_>>()})
        })
    })
    .await;
    let name = run
        .call("jinjier.page", jinjier.discover_database_name(), debug_class, |name| {
            json!(name)
        })
        .await;
    if let Some(name) = name {
        let database = run
            .call(
                "jinjier.database",
                jinjier.download_database(&name),
                debug_class,
                |body| json!({"bytes": body.len()}),
            )
            .await;
        if let Some(database) = database {
            let parser = AvidParser::new(AvidConfig::default()).map_err(|error| format!("{error:?}"))?;
            match jinjier::parse_lists(&database, &|title| parser.get_avid(title)) {
                Ok(lists) => run.push(
                    "jinjier.lists",
                    &Value::Array(
                        lists
                            .iter()
                            .map(|list| {
                                json!({"key": list.key, "kind": list.kind, "note": list.note,
                                       "entries": list.entries.len(),
                                       "first": list.entries.iter().take(3).map(|entry| json!({
                                           "rank": entry.rank, "avid": entry.avid, "title": entry.title}))
                                           .collect::<Vec<_>>()})
                            })
                            .collect(),
                    ),
                ),
                Err(error) => run.results.push(json!({"step": "jinjier.lists", "error": {
                    "class": debug_class(&error), "message": error.to_string()}})),
            }
        }
    }
    let body = run
        .call(
            "rsshub.feed",
            feeds.fetch(RSSHUB_FEED),
            debug_class,
            |body| json!({"bytes": body.len()}),
        )
        .await;
    if let Some(body) = body {
        match embyx_monitor::feeds::parse_feed(&body) {
            Ok(feed) => run.push(
                "rsshub.parsed",
                &json!({"title": feed.title, "items": feed.items.len(),
                       "keys": feed.items.iter().take(10).map(|item| item.key.clone()).collect::<Vec<_>>(),
                       "titles": feed.items.iter().take(10).map(|item| item.title.clone()).collect::<Vec<_>>()}),
            ),
            Err(error) => run.results.push(json!({"step": "rsshub.parsed", "error": {
                "class": "FeedParseError", "message": error.to_string()}})),
        }
    }
    Ok(run.results)
}

#[allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a command-line run reports on stdout"
)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("level3: {error}");
            return std::process::ExitCode::from(2);
        }
    };
    let started = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let results = match run(&args).await {
        Ok(results) => results,
        Err(error) => {
            eprintln!("level3: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    for entry in &results {
        let state = entry
            .get("error")
            .map_or_else(|| "ok".to_owned(), |error| format!("ERROR {}", error["class"]));
        println!("{} {state}", entry["step"].as_str().unwrap_or_default());
    }
    let document = json!({"side": "rust", "started": started, "gap": args.gap, "results": results});
    let text = serde_json::to_string_pretty(&document).unwrap_or_default() + "\n";
    if let Err(error) = std::fs::write(&args.out, text) {
        eprintln!("level3: {}: {error}", args.out.display());
        return std::process::ExitCode::FAILURE;
    }
    std::process::ExitCode::SUCCESS
}
