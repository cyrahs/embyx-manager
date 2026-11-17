//! A scripted web for the clients' tests, standing in for httpx's `MockTransport`.
//!
//! The sources' clients are pointed at it as their HTTP proxy, so a request for
//! `http://wx4.sinaimg.cn/large/abc.jpg` arrives here with its full URL and the handler
//! answers for any host, as Python's mock transport does. It speaks HTTP/1.1 by hand so a
//! handler can also drop the connection or stall, which is how transport errors are made.
//!
//! An `https://` request arrives as a `CONNECT` tunnel; the fake web answers the TLS
//! handshake itself with a certificate for the host signed by a test authority, and a
//! client built with [`test_roots`] trusts it. The request is then seen as `https://...`.
//!
//! Copied from fav (`fav-sources/src/testutil.rs`); see docs/CONVENTIONS.md, "Code copied from fav". `D` numbers
//! marked fav's are fav's own DIVERGENCES.md entries.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use rcgen::{CertificateParams, Issuer, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// The test authority the fake web signs its `https://` sites with.
struct Authority {
    params: CertificateParams,
    key: KeyPair,
    der: Vec<u8>,
    sites: Mutex<HashMap<String, Arc<rustls::ServerConfig>>>,
}

static AUTHORITY: LazyLock<Authority> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "embyx fake web");
    let der = params.self_signed(&key).expect("authority").der().to_vec();
    Authority {
        params,
        key,
        der,
        sites: Mutex::new(HashMap::new()),
    }
});

impl Authority {
    /// The server side of a TLS handshake for `host`.
    fn site(&self, host: &str) -> Arc<rustls::ServerConfig> {
        let mut sites = self.sites.lock().expect("lock");
        let config = sites.entry(host.to_owned()).or_insert_with(|| {
            let key = KeyPair::generate().expect("key");
            let leaf = CertificateParams::new(vec![host.to_owned()])
                .expect("params")
                .signed_by(&key, &Issuer::from_params(&self.params, &self.key))
                .expect("certificate");
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(leaf.der().to_vec())],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
                )
                .expect("server config");
            Arc::new(config)
        });
        Arc::clone(config)
    }
}

/// The roots a client trusts to reach the fake web's `https://` sites.
pub fn test_roots() -> Vec<Vec<u8>> {
    vec![AUTHORITY.der.clone()]
}

/// One request as the fake web saw it.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: String,
    /// The absolute URL (`http://host/path?query`, or `https://` through a tunnel).
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn host(&self) -> &str {
        let rest = self.url.split_once("://").map_or(self.url.as_str(), |(_, rest)| rest);
        rest.split(['/', '?', ':']).next().unwrap_or_default()
    }

    pub fn path(&self) -> &str {
        let rest = self.url.split_once("://").map_or(self.url.as_str(), |(_, rest)| rest);
        let path = rest.find('/').map_or("", |index| &rest[index..]);
        path.split(['?', '#']).next().unwrap_or_default()
    }

    pub fn query(&self) -> &str {
        self.url
            .split_once('?')
            .map_or("", |(_, query)| query.split('#').next().unwrap_or_default())
    }
}

/// What the fake web does with a request.
#[derive(Debug, Clone)]
pub enum Reply {
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// Close the connection without answering (httpx: `RemoteProtocolError`).
    Close,
    /// Answer nothing for a while, then close (a read timeout on the client side).
    Stall(Duration),
    /// Reset the connection (TCP RST) without answering (httpx: `ReadError`).
    Reset,
    /// Answer with a `content-length` of `length` but send only `sent`, then close
    /// (httpx: `RemoteProtocolError`, "peer closed connection without sending complete
    /// message body").
    Truncated { status: u16, sent: Vec<u8>, length: usize },
}

impl Reply {
    pub fn status(status: u16) -> Self {
        Self::Respond {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn bytes(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self::Respond {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn json(status: u16, body: &serde_json::Value) -> Self {
        Self::Respond {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: serde_json::to_vec(body).expect("JSON"),
        }
    }
}

type Handler = Arc<dyn Fn(&Seen) -> Reply + Send + Sync>;

/// The fake web: a proxy address and the requests it served.
#[derive(Clone)]
pub struct FakeWeb {
    pub proxy: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl std::fmt::Debug for FakeWeb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeWeb")
            .field("proxy", &self.proxy)
            .finish_non_exhaustive()
    }
}

impl FakeWeb {
    pub async fn start(handler: impl Fn(&Seen) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Handler = Arc::new(handler);
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let handler = Arc::clone(&handler);
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    serve(stream, handler, recorded).await;
                });
            }
        });
        Self {
            proxy: format!("http://{address}"),
            seen,
        }
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock").clone()
    }

    pub fn urls(&self) -> Vec<String> {
        self.seen().into_iter().map(|seen| seen.url).collect()
    }
}

/// A connection the fake web answers on: plain, or TLS inside a tunnel.
trait Connection: AsyncRead + AsyncWrite + Unpin + Send {
    /// Drop the connection with a TCP reset.
    fn reset(self);
}

impl Connection for TcpStream {
    fn reset(self) {
        let _ = self.set_zero_linger();
    }
}

impl Connection for tokio_rustls::server::TlsStream<TcpStream> {
    fn reset(self) {
        let (stream, _) = self.into_inner();
        let _ = stream.set_zero_linger();
    }
}

async fn serve(mut stream: TcpStream, handler: Handler, seen: Arc<Mutex<Vec<Seen>>>) {
    let Some((head, rest)) = read_head(&mut stream).await else {
        return;
    };
    if head.starts_with("CONNECT ") {
        let authority = head.split(' ').nth(1).unwrap_or_default().to_owned();
        let host = authority
            .rsplit_once(':')
            .map_or(authority.as_str(), |(host, _)| host)
            .to_owned();
        if stream
            .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .await
            .is_err()
        {
            return;
        }
        let acceptor = tokio_rustls::TlsAcceptor::from(AUTHORITY.site(&host));
        let Ok(mut tls) = acceptor.accept(stream).await else {
            return;
        };
        let Some((head, rest)) = read_head(&mut tls).await else {
            return;
        };
        answer(tls, "https", &head, rest, &handler, &seen).await;
        return;
    }
    answer(stream, "http", &head, rest, &handler, &seen).await;
}

/// The request head, and whatever of the body came with it.
async fn read_head(stream: &mut (impl AsyncRead + Unpin)) -> Option<(String, Vec<u8>)> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    let header_end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    Some((head, buffer[header_end + 4..].to_vec()))
}

async fn answer(
    mut stream: impl Connection,
    scheme: &str,
    head: &str,
    mut body: Vec<u8>,
    handler: &Handler,
    seen: &Mutex<Vec<Seen>>,
) {
    let mut chunk = [0_u8; 8192];
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);
    while body.len() < length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }
    let url = if target.starts_with("http://") || target.starts_with("https://") {
        target
    } else {
        let host = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("host"))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        format!("{scheme}://{host}{target}")
    };
    let request = Seen {
        method,
        url,
        headers,
        body,
    };
    let reply = handler(&request);
    seen.lock().expect("lock").push(request);
    match reply {
        Reply::Close => {}
        Reply::Reset => stream.reset(),
        Reply::Truncated { status, sent, length } => {
            let head = format!("HTTP/1.1 {status} OK\r\nconnection: close\r\ncontent-length: {length}\r\n\r\n");
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&sent).await;
            let _ = stream.flush().await;
        }
        Reply::Stall(duration) => tokio::time::sleep(duration).await,
        Reply::Respond { status, headers, body } => {
            let reason = http::StatusCode::from_u16(status)
                .ok()
                .and_then(|status| status.canonical_reason())
                .unwrap_or("");
            let mut response = format!(
                "HTTP/1.1 {status} {reason}\r\nconnection: close\r\ncontent-length: {}\r\n",
                body.len()
            );
            for (name, value) in headers {
                let _ = write!(response, "{name}: {value}\r\n");
            }
            response.push_str("\r\n");
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        }
    }
}
