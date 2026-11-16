//! The client against a local server: redirects, history, status errors, transport errors.

use std::net::SocketAddr;

use axum::Router;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use super::{ClientOptions, HttpClient, Request};
use crate::net::ErrorClass;

async fn serve(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    address
}

fn redirect(status: StatusCode, location: &str, extra: &[(&'static str, &'static str)]) -> Response {
    let mut response = (status, [(header::LOCATION, location.to_owned())]).into_response();
    for (name, value) in extra {
        response.headers_mut().insert(*name, value.parse().expect("value"));
    }
    response
}

fn following() -> HttpClient {
    HttpClient::new(&ClientOptions {
        follow_redirects: true,
        headers: vec![("User-Agent".to_owned(), "embyx-test".to_owned())],
        ..ClientOptions::default()
    })
    .expect("client")
}

#[tokio::test]
async fn redirects_are_followed_and_kept_in_history() {
    let router = Router::new()
        .route(
            "/a",
            get(|| async { redirect(StatusCode::MOVED_PERMANENTLY, "/b#101", &[("x-image-errno", "101")]) }),
        )
        .route(
            "/b",
            get(|| async { redirect(StatusCode::FOUND, "//127.0.0.1:1/never", &[]) }),
        )
        .route(
            "/ua",
            get(|headers: HeaderMap| async move {
                headers
                    .get("user-agent")
                    .map(|v| v.to_str().unwrap_or_default().to_owned())
                    .unwrap_or_default()
            }),
        );
    let address = serve(router.clone()).await;
    let client = following();

    let response = client.get(&format!("http://{address}/ua")).await.expect("response");
    assert_eq!(response.bytes().await.expect("body"), "embyx-test");

    let no_follow = HttpClient::new(&ClientOptions::default()).expect("client");
    let response = no_follow.get(&format!("http://{address}/a")).await.expect("response");
    assert_eq!(response.status(), 301);
    assert!(response.history().is_empty());
    let error = response.error_for_status().expect_err("301 is not a success");
    assert_eq!(
        error.to_string(),
        format!(
            "Redirect response '301 Moved Permanently' for url 'http://{address}/a'\nRedirect location: '/b#101'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/301"
        )
    );

    // The second hop points at a closed port: the failure names the host, not the URL.
    let error = client.get(&format!("http://{address}/a")).await.expect_err("refused");
    assert_eq!(error.class, ErrorClass::ConnectError);
    assert_eq!(error.host, "127.0.0.1");
    assert!(!error.message.contains("never"), "{}", error.message);
}

#[tokio::test]
async fn history_and_final_url_are_reported() {
    let router = Router::new()
        .route(
            "/start",
            get(|| async {
                redirect(
                    StatusCode::MOVED_PERMANENTLY,
                    "/images/default_x.gif#101",
                    &[("x-image-errno", "101")],
                )
            }),
        )
        .route("/images/default_x.gif", get(|| async { "GIF89a" }))
        .route("/missing", get(|| async { StatusCode::NOT_FOUND }))
        .route("/loop", get(|| async { redirect(StatusCode::FOUND, "/loop", &[]) }));
    let address = serve(router).await;
    let client = following();

    let response = client.get(&format!("http://{address}/start")).await.expect("response");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.url().as_str(),
        format!("http://{address}/images/default_x.gif#101")
    );
    assert_eq!(response.history().len(), 1);
    assert_eq!(response.history()[0].status, 301);
    assert_eq!(
        response.history()[0].headers.get("x-image-errno").expect("errno"),
        "101"
    );

    let response = client
        .get(&format!("http://{address}/missing"))
        .await
        .expect("response");
    let error = response.error_for_status().expect_err("404");
    assert_eq!(
        error.to_string(),
        format!(
            "Client error '404 Not Found' for url 'http://{address}/missing'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404"
        )
    );

    let error = client.get(&format!("http://{address}/loop")).await.expect_err("loops");
    assert_eq!(error.class, ErrorClass::TooManyRedirects);
    assert_eq!(error.message, "Exceeded maximum allowed redirects.");
}

#[tokio::test]
async fn a_post_redirected_by_302_becomes_a_get() {
    let router = Router::new()
        .route(
            "/form",
            axum::routing::post(|| async { redirect(StatusCode::FOUND, "/done", &[]) }),
        )
        .route("/done", get(|| async { "got" }));
    let address = serve(router).await;
    let response = following()
        .send(Request::post(format!("http://{address}/form"), b"data=1".to_vec()))
        .await
        .expect("response");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body"), "got");
}

/// Set in the child process of [`trust_env_false_ignores_the_environment_proxy`].
const TRUST_ENV_CHILD: &str = "EMBYX_NET_TRUST_ENV_CHILD";
const PROXY_VARIABLES: [&str; 8] = [
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

/// The half of the test that runs with `HTTP_PROXY` pointing at the parent's proxy: a
/// client that trusts the environment reaches a host through it, one with
/// `trust_env: false` goes direct (and finds no such host). Outside the child it does
/// nothing.
#[tokio::test]
async fn trust_env_child() {
    if std::env::var_os(TRUST_ENV_CHILD).is_none() {
        return;
    }
    let client = |trust_env: bool| {
        HttpClient::new(&ClientOptions {
            trust_env,
            ..ClientOptions::default()
        })
        .expect("client")
    };
    let proxied = client(true)
        .get("http://embyx-proxy-test.invalid/")
        .await
        .expect("through the proxy");
    assert_eq!(proxied.bytes().await.expect("body").as_ref(), b"proxied");
    let direct = client(false).get("http://embyx-proxy-test.invalid/").await;
    assert!(
        direct.is_err(),
        "a client that ignores the environment went through the proxy"
    );
}

#[tokio::test]
async fn trust_env_false_ignores_the_environment_proxy() {
    if std::env::var_os(TRUST_ENV_CHILD).is_some() {
        return;
    }
    let proxy = serve(Router::new().fallback(|| async { "proxied" })).await;
    let child = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(std::env::current_exe().expect("test binary"));
        command.args(["--exact", "net::client::tests::trust_env_child", "--quiet"]);
        for name in PROXY_VARIABLES {
            command.env_remove(name);
        }
        command
            .env("HTTP_PROXY", format!("http://{proxy}"))
            .env(TRUST_ENV_CHILD, "1");
        command.output().expect("child runs")
    })
    .await
    .expect("joined");
    let output = String::from_utf8_lossy(&child.stdout).into_owned() + &String::from_utf8_lossy(&child.stderr);
    assert!(child.status.success(), "{output}");
    assert!(output.contains("1 passed"), "{output}");
}

/// A server that keeps each request's head (up to the blank line) as it arrived on the wire and answers with
/// `answer(index)`, one connection per request.
async fn capture(
    answer: impl Fn(usize) -> String + Send + Sync + 'static,
) -> (SocketAddr, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = std::sync::Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut head = Vec::new();
            let mut buffer = [0u8; 4096];
            while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buffer).await.expect("read");
                if read == 0 {
                    break;
                }
                head.extend_from_slice(&buffer[..read]);
            }
            let index = {
                let mut seen = kept.lock().expect("lock");
                let end = head
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(head.len(), |at| at + 4);
                seen.push(String::from_utf8_lossy(&head[..end]).into_owned());
                seen.len() - 1
            };
            socket.write_all(answer(index).as_bytes()).await.expect("write");
            socket.shutdown().await.ok();
        }
    });
    (address, seen)
}

const EMPTY_200: &str = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// fav's D182: Cloudflare refused Hanime1's requests while the names were lowercase and `Host`
/// came last. The heads below are what httpx 0.28.1 wrote for the same calls, captured
/// from `httpx.Client` against a socket.
#[tokio::test]
async fn requests_go_out_in_httpx_header_order_and_case() {
    let (address, seen) = capture(|_| EMPTY_200.to_owned()).await;
    let ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";
    // `Hanime1.__init__` and `_build_page_headers`.
    let client = HttpClient::new(&ClientOptions {
        headers: vec![
            ("User-Agent".to_owned(), ua.to_owned()),
            ("Accept-Language".to_owned(), "en-US,en;q=0.9".to_owned()),
        ],
        follow_redirects: true,
        ..ClientOptions::default()
    })
    .expect("client");
    let request = Request::get(format!("http://{address}/"))
        .header("Referer", "https://hanime1.me/")
        .header("User-Agent", ua)
        .header(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        )
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        .header("Cookie", "user_lang=zh-CHS");
    client.send(request).await.expect("response");
    // A client with no headers of its own, posting a body.
    HttpClient::new(&ClientOptions::default())
        .expect("client")
        .send(Request::post(format!("http://{address}/p"), b"data=1".to_vec()))
        .await
        .expect("response");
    let seen = seen.lock().expect("lock").clone();
    assert_eq!(
        seen[0],
        format!(
            "GET / HTTP/1.1\r\nHost: {address}\r\nAccept-Encoding: gzip, deflate\r\nConnection: keep-alive\r\n\
             Referer: https://hanime1.me/\r\nUser-Agent: {ua}\r\n\
             Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8\r\n\
             Accept-Language: zh-CN,zh;q=0.9,en;q=0.8\r\nCookie: user_lang=zh-CHS\r\n\r\n"
        )
    );
    assert_eq!(
        seen[1],
        format!(
            "POST /p HTTP/1.1\r\nHost: {address}\r\nAccept: */*\r\nAccept-Encoding: gzip, deflate\r\n\
             Connection: keep-alive\r\nUser-Agent: python-httpx/0.28.1\r\nContent-Length: 6\r\n\r\n"
        )
    );
}

/// A redirect to another origin keeps `Host` first with the new authority and drops
/// `Authorization`, as `_redirect_headers` does.
#[tokio::test]
async fn a_redirect_to_another_origin_rewrites_host_in_place() {
    let (target, target_seen) = capture(|_| EMPTY_200.to_owned()).await;
    let (start, start_seen) = capture(move |_| {
        format!("HTTP/1.1 302 Found\r\nLocation: http://{target}/b\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
    })
    .await;
    HttpClient::new(&ClientOptions {
        follow_redirects: true,
        ..ClientOptions::default()
    })
    .expect("client")
    .send(
        Request::get(format!("http://{start}/a"))
            .header("X-Requested-With", "XMLHttpRequest")
            .header("Authorization", "Bearer t"),
    )
    .await
    .expect("response");
    let defaults = "Accept: */*\r\nAccept-Encoding: gzip, deflate\r\nConnection: keep-alive\r\nUser-Agent: python-httpx/0.28.1\r\n";
    assert_eq!(
        start_seen.lock().expect("lock")[0],
        format!(
            "GET /a HTTP/1.1\r\nHost: {start}\r\n{defaults}X-Requested-With: XMLHttpRequest\r\nAuthorization: Bearer t\r\n\r\n"
        )
    );
    assert_eq!(
        target_seen.lock().expect("lock")[0],
        format!("GET /b HTTP/1.1\r\nHost: {target}\r\n{defaults}X-Requested-With: XMLHttpRequest\r\n\r\n")
    );
}

/// A `Host` the caller sets still goes out first, as h11 writes it.
#[tokio::test]
async fn a_host_the_caller_sets_goes_first() {
    let (address, seen) = capture(|_| EMPTY_200.to_owned()).await;
    HttpClient::new(&ClientOptions::default())
        .expect("client")
        .send(
            Request::get(format!("http://{address}/"))
                .header("X-A", "1")
                .header("Host", "example.test"),
        )
        .await
        .expect("response");
    assert_eq!(
        seen.lock().expect("lock")[0],
        "GET / HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\nAccept-Encoding: gzip, deflate\r\nConnection: keep-alive\r\n\
         User-Agent: python-httpx/0.28.1\r\nX-A: 1\r\n\r\n"
    );
}

/// `body` under a content coding: `zlib` and `bare` are the two forms servers send as
/// `deflate` (RFC 1950 around RFC 1951, and RFC 1951 alone).
fn coded(body: &[u8], form: &str) -> Vec<u8> {
    use std::io::Write;

    let level = flate2::Compression::default();
    match form {
        "zlib" => {
            let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), level);
            encoder.write_all(body).expect("write");
            encoder.finish().expect("finish")
        }
        "bare" => {
            let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), level);
            encoder.write_all(body).expect("write");
            encoder.finish().expect("finish")
        }
        _ => {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
            encoder.write_all(body).expect("write");
            encoder.finish().expect("finish")
        }
    }
}

// httpx's `DeflateDecoder` (`httpx/_decoders.py`): zlib first, the bare stream when the first
// bytes are not a zlib header. Emby answers `Accept-Encoding: gzip, deflate` with the bare form.
// No Python test covers it, so this is a Rust-only test.
#[tokio::test]
async fn a_deflate_body_is_decoded_in_both_forms_like_httpx() {
    // Varied enough to stay several network chunks long once compressed.
    let body: Vec<u8> = (0..120_000_u32)
        .flat_map(|n| format!("{n} {}\n", n.wrapping_mul(2_654_435_761)).into_bytes())
        .collect();
    let answer = |coding: &'static str, bytes: Vec<u8>| {
        get(move || {
            let bytes = bytes.clone();
            async move { ([(header::CONTENT_ENCODING, coding)], bytes) }
        })
    };
    let zlib = coded(&body, "zlib");
    let router = Router::new()
        .route("/zlib", answer("deflate", zlib.clone()))
        .route("/bare", answer("deflate", coded(&body, "bare")))
        .route("/upper", answer("Deflate", coded(&body, "bare")))
        .route("/gzip", answer("gzip", coded(&body, "gzip")))
        .route("/cut", answer("deflate", zlib[..zlib.len() / 2].to_vec()))
        .route("/junk", answer("deflate", b"\xff\xff not a deflate stream".to_vec()))
        .route("/empty", answer("deflate", Vec::new()));
    let address = serve(router).await;
    let client = following();
    let url = |path: &str| format!("http://{address}{path}");

    for path in ["/zlib", "/bare", "/upper", "/gzip"] {
        let whole = client.send(Request::get(url(path))).await.expect("send");
        // httpx leaves the header in place for the codings it decodes itself.
        if path != "/gzip" {
            assert_eq!(
                whole.header("content-encoding").map(|coding| coding.to_lowercase()),
                Some("deflate".to_owned())
            );
        }
        assert!(whole.bytes().await.expect("bytes") == body, "{path} as a whole");

        let mut pieces = client.send(Request::get(url(path))).await.expect("send");
        let (mut read, mut count) = (Vec::new(), 0);
        while let Some(piece) = pieces.chunk().await.expect("chunk") {
            assert!(!piece.is_empty(), "{path}");
            read.extend_from_slice(&piece);
            count += 1;
        }
        assert!(read == body, "{path} in pieces");
        assert!(count > 1, "{path} came in one piece, so the pieces were not tested");
    }

    // A stream cut short gives what it held (zlib's `decompressobj` raises nothing at the end):
    // half of the stream holds about half of a body as even as this one.
    let cut = client.send(Request::get(url("/cut"))).await.expect("send");
    let held = cut.bytes().await.expect("bytes");
    assert!(
        held.len() > body.len() / 3 && body.starts_with(&held),
        "{} bytes",
        held.len()
    );

    let empty = client.send(Request::get(url("/empty"))).await.expect("send");
    assert!(empty.bytes().await.expect("bytes").is_empty());

    // Not a zlib header, so the bare form is tried, and that fails with zlib's own reason.
    let refused = "Error -3 while decompressing data: invalid block type";
    let junk = client.send(Request::get(url("/junk"))).await.expect("send");
    let error = junk.bytes().await.expect_err("junk is refused");
    assert_eq!(
        (error.class, error.message.as_str()),
        (ErrorClass::DecodingError, refused)
    );
    let mut junk = client.send(Request::get(url("/junk"))).await.expect("send");
    let error = junk.chunk().await.expect_err("junk is refused in pieces");
    assert_eq!(
        (error.class, error.message.as_str()),
        (ErrorClass::DecodingError, refused)
    );
}

fn deflate_decoder() -> super::Deflate {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_ENCODING, "deflate".parse().expect("value"));
    super::Deflate::of(&headers).expect("a deflate response")
}

/// `stream` through one decoder, `size` bytes at a time.
fn decoded_in(stream: &[u8], size: usize) -> Result<Vec<u8>, String> {
    let mut decoder = deflate_decoder();
    let mut read = Vec::new();
    for piece in stream.chunks(size) {
        read.extend(decoder.decode(piece)?);
    }
    Ok(read)
}

// The piece boundaries a local server cannot set, each checked against httpx 0.28.1's
// `DeflateDecoder` by hand. Rust-only test.
#[test]
fn the_deflate_decoder_holds_at_any_piece_boundary_like_httpx() {
    let body: Vec<u8> = (0..20_000_u32)
        .flat_map(|n| format!("{n} {}\n", n.wrapping_mul(2_654_435_761)).into_bytes())
        .collect();
    let (zlib, bare) = (coded(&body, "zlib"), coded(&body, "bare"));
    let header_check = "Error -3 while decompressing data: incorrect header check";

    // The zlib form survives any split, its two header bytes apart included.
    for size in [1, 2, 3, 7, 4096, zlib.len()] {
        assert!(decoded_in(&zlib, size).as_deref() == Ok(&body[..]), "zlib by {size}");
    }
    // The bare form is recognised on the first piece only, and one byte is not yet a wrong
    // header: httpx fails such a body the same way.
    for size in [2, 3, 7, 4096, bare.len()] {
        assert!(decoded_in(&bare, size).as_deref() == Ok(&body[..]), "bare by {size}");
    }
    assert_eq!(decoded_in(&bare, 1), Err(header_check.to_owned()));
    let mut decoder = deflate_decoder();
    assert_eq!(decoder.decode(b""), Ok(Vec::new()));
    assert_eq!(decoder.decode(&bare), Err(header_check.to_owned()));

    // Bytes after the stream's end are dropped, in the same piece or a later one.
    for stream in [&zlib, &bare] {
        let mut longer = stream.clone();
        longer.extend_from_slice(&zlib);
        longer.extend_from_slice(b"and more");
        for size in [7, longer.len()] {
            assert!(
                decoded_in(&longer, size).as_deref() == Ok(&body[..]),
                "trailing by {size}"
            );
        }
    }

    // A cut stream gives the same prefix however it arrives.
    let cut = &zlib[..zlib.len() / 2];
    let held = decoded_in(cut, cut.len()).expect("a prefix");
    assert!(
        held.len() > body.len() / 3 && body.starts_with(&held),
        "{} bytes",
        held.len()
    );
    assert!(decoded_in(cut, 1).as_ref() == Ok(&held), "cut, byte by byte");

    // An error on a later piece: stored blocks, the second one's length check broken. The first
    // piece is given out and the second fails.
    let stored = {
        use std::io::Write;
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::none());
        encoder.write_all(&body).expect("write");
        encoder.finish().expect("finish")
    };
    assert_eq!(stored[2] & 0b111, 0, "a stored block that is not the last");
    let first = usize::from(u16::from_le_bytes([stored[3], stored[4]]));
    let second = 2 + 5 + first;
    let mut broken = stored.clone();
    broken[second + 3] ^= 0xff;
    let mut decoder = deflate_decoder();
    assert!(
        decoder.decode(&broken[..second]).as_deref() == Ok(&body[..first]),
        "the first block"
    );
    assert_eq!(
        decoder.decode(&broken[second..]),
        Err("Error -3 while decompressing data: invalid stored block lengths".to_owned())
    );
    // The same with the checksum at the end broken: read again as a bare stream, the second
    // piece would be whole blocks and decode, so this fails only because nothing is retried.
    let mut unsummed = stored.clone();
    *unsummed.last_mut().expect("a checksum") ^= 0xff;
    let mut decoder = deflate_decoder();
    assert!(
        decoder.decode(&unsummed[..second]).as_deref() == Ok(&body[..first]),
        "the first block"
    );
    assert_eq!(
        decoder.decode(&unsummed[second..]),
        Err("Error -3 while decompressing data: incorrect data check".to_owned())
    );
}
