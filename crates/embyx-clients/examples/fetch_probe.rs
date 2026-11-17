//! One GET with a Rust client, for the fingerprint probe (not part of the application).
//! `tools/probes/tls_fingerprint.py` points it at its own local listener and compares what a
//! server sees with `curl_cffi`'s (docs/rewrite/API-COVERAGE.md). Kept after the rewrite (WP16)
//! as the offline check to repeat when `wreq`, its Chrome emulation or `curl_cffi` is upgraded;
//! the live probe pod's tooling was removed with the pod approval it needed.
//!
//!     cargo run -p embyx-clients --example fetch_probe -- <client> <url> \
//!         [--root CERT.der] [--body PATH] [-H 'Name: value']...
//!
//! Clients:
//! - `wreq`: wreq with `wreq_util::Emulation::Chrome149` as it comes; `-H` headers are set
//!   per request, as `clients/avbase.py` adds its two to `curl_cffi`'s.
//! - `wreq-tuned`: the same TLS and HTTP/2 settings, but the default header list, its order
//!   and the HEADERS priority (weight 256) copied from what `curl_cffi` 0.16.3
//!   `impersonate='chrome'` sends; an `-H` header replaces a default in place or is appended,
//!   as `curl_cffi` merges a session's headers.
//! - `httpx`: `embyx_clients::net::HttpClient`, the httpx-shaped reqwest client (rustls).
//! - `avbase` (WP2): the session `AvbaseClient::new` builds for the default host
//!   (`avbase::session::ImpersonatingSession`: `wreq-tuned` with the client's own `Referer`
//!   and `Accept-Language`, cookies kept, redirects followed); `-H` is ignored.
//! - `javbus` (WP2): the client `JavBusClient::new` builds (`JavBusClient::client_options`:
//!   the httpx-shaped client with its `Accept-Language` and `Cookie`); `-H` is ignored.
//!
//! The WP0 candidates follow no redirect and never retry: the live run counts every request it
//! sends. `wreq-tuned` takes its headers and HEADERS priority from
//! `avbase::session::chrome_emulation`, which the application uses as well.
//! Prints one JSON object: the status, a few response headers (never cookies), the final URL,
//! the body's size, or the error. `--body` writes the body to a file for the caller to judge.
//! `--root` trusts that certificate (DER) instead of the usual roots; the wreq clients then
//! skip verification altogether (they only ever dial the local listener that way).

use std::process::ExitCode;
use std::time::Duration;

use embyx_clients::avbase::session::{ImpersonatingSession, chrome_emulation};
use embyx_clients::avbase::{AvbaseClient, AvbaseOptions, AvbaseSession as _};
use embyx_clients::javbus::{JavBusClient, JavBusOptions};
use embyx_clients::net::{ClientOptions, HttpClient, Request};
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};

const TIMEOUT: Duration = Duration::from_secs(30);
/// Response headers worth recording: who answered and whether Cloudflare challenged.
const KEPT_HEADERS: [&str; 5] = ["server", "cf-mitigated", "content-type", "location", "cf-cache-status"];

struct Args {
    client: String,
    url: String,
    root: Option<Vec<u8>>,
    body: Option<String>,
    headers: Vec<(String, String)>,
}

fn parse_args() -> Result<Args, String> {
    let mut rest = std::env::args().skip(1);
    let client = rest.next().ok_or("missing <client>")?;
    let url = rest.next().ok_or("missing <url>")?;
    let mut args = Args {
        client,
        url,
        root: None,
        body: None,
        headers: Vec::new(),
    };
    while let Some(flag) = rest.next() {
        let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--root" => args.root = Some(std::fs::read(&value).map_err(|error| format!("{value}: {error}"))?),
            "--body" => args.body = Some(value),
            "-H" => {
                let (name, header) = value.split_once(':').ok_or_else(|| format!("bad header {value:?}"))?;
                args.headers.push((name.trim().to_owned(), header.trim().to_owned()));
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(args)
}

struct Outcome {
    status: u16,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn kept(name: &str) -> bool {
    KEPT_HEADERS.contains(&name)
}

fn wreq_client(args: &Args, tuned: bool) -> Result<wreq::Client, String> {
    let emulation = if tuned {
        let headers: Vec<(&str, &str)> = args
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        chrome_emulation(&headers)?
    } else {
        wreq::IntoEmulation::into_emulation(wreq_util::Emulation::Chrome149)
    };
    wreq::Client::builder()
        .emulation(emulation)
        .timeout(TIMEOUT)
        // One request is one request: no redirect followed, no retry (the live run counts them).
        .redirect(wreq::redirect::Policy::none())
        .retry(wreq::retry::Policy::never())
        .tls_cert_verification(args.root.is_none())
        .build()
        .map_err(|error| error.to_string())
}

async fn fetch_wreq(args: &Args, tuned: bool) -> Result<Outcome, String> {
    let client = wreq_client(args, tuned)?;
    let mut request = client.get(&args.url);
    if !tuned {
        for (name, value) in &args.headers {
            request = request.header(name.as_str(), value.as_str());
        }
    }
    let response = request.send().await.map_err(|error| format!("{error:?}"))?;
    let status = response.status().as_u16();
    let url = response.uri().to_string();
    let headers = response
        .headers()
        .iter()
        .filter(|(name, _)| kept(name.as_str()))
        .map(|(name, value)| (name.to_string(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
        .collect();
    let body = response.bytes().await.map_err(|error| format!("{error:?}"))?.to_vec();
    Ok(Outcome {
        status,
        url,
        headers,
        body,
    })
}

/// The session `AvbaseClient::new` builds, trusting `--root`.
async fn fetch_avbase(args: &Args) -> Result<Outcome, String> {
    let options = AvbaseOptions {
        timeout: TIMEOUT,
        ..AvbaseOptions::default()
    };
    let session = ImpersonatingSession::new(&embyx_clients::avbase::session::SessionOptions {
        roots: args.root.iter().cloned().collect(),
        ..AvbaseClient::session_options(&options)
    })?;
    let response = session.get(&args.url, &[]).await.map_err(|error| error.to_string())?;
    Ok(Outcome {
        status: response.status,
        url: args.url.clone(),
        headers: response
            .content_type
            .map(|value| ("content-type".to_owned(), value))
            .into_iter()
            .collect(),
        body: response.body,
    })
}

async fn fetch_httpx(args: &Args, configured: Option<ClientOptions>) -> Result<Outcome, String> {
    let roots: Vec<Vec<u8>> = args.root.iter().cloned().collect();
    let options = match configured {
        Some(options) => ClientOptions { roots, ..options },
        None => ClientOptions {
            timeout: TIMEOUT,
            roots,
            ..ClientOptions::default()
        },
    };
    let client = HttpClient::new(&options).map_err(|error| error.to_string())?;
    let mut request = Request::get(args.url.clone());
    if !matches!(args.client.as_str(), "javbus") {
        for (name, value) in &args.headers {
            request = request.header(name.clone(), value.clone());
        }
    }
    let response = client.send(request).await.map_err(|error| error.to_string())?;
    let status = response.status();
    let url = response.url().to_string();
    let headers = response
        .headers()
        .iter()
        .filter(|(name, _)| kept(name.as_str()))
        .map(|(name, value)| (name.to_string(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
        .collect();
    let body = response.bytes().await.map_err(|error| error.to_string())?.to_vec();
    Ok(Outcome {
        status,
        url,
        headers,
        body,
    })
}

#[allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a command-line probe reports on stdout"
)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("fetch_probe: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = match args.client.as_str() {
        "wreq" => fetch_wreq(&args, false).await,
        "wreq-tuned" => fetch_wreq(&args, true).await,
        "httpx" => fetch_httpx(&args, None).await,
        "avbase" => fetch_avbase(&args).await,
        "javbus" => {
            let options = JavBusOptions {
                timeout: TIMEOUT,
                ..JavBusOptions::default()
            };
            fetch_httpx(&args, Some(JavBusClient::client_options(&options))).await
        }
        other => Err(format!("unknown client {other}")),
    };
    let mut report = Dict::new();
    report.insert("client", Value::Str(args.client.clone()));
    match result {
        Ok(outcome) => {
            report.insert("status", Value::Int(outcome.status.into()));
            report.insert("url", Value::Str(outcome.url));
            let mut headers = Dict::new();
            for (name, value) in outcome.headers {
                headers.insert(name, Value::Str(value));
            }
            report.insert("headers", Value::Dict(headers));
            report.insert(
                "body_bytes",
                Value::Int(outcome.body.len().try_into().unwrap_or(i128::MAX)),
            );
            if let Some(path) = &args.body
                && let Err(error) = std::fs::write(path, &outcome.body)
            {
                eprintln!("fetch_probe: {path}: {error}");
                return ExitCode::FAILURE;
            }
        }
        Err(error) => report.insert("error", Value::Str(error)),
    }
    println!(
        "{}",
        pyjson::dumps(&Value::Dict(report), &DumpOptions::compact()).expect("json")
    );
    ExitCode::SUCCESS
}
