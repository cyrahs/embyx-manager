//! API-coverage probes for the clients crate (docs/rewrite/API-COVERAGE.md, "tonic",
//! "prost-reflect", "scraper" and "wreq").
//!
//! - tonic: the stubs generated from the baseline's descriptor talk to the in-process fake
//!   CloudDrive (`clouddrive::fake`) over a real socket, with the bearer metadata
//!   `client.py` sends, and the status codes and details it maps (`NOT_FOUND`,
//!   `INVALID_ARGUMENT` with "can't open a file as directory").
//! - prost-reflect: the fake's records match `MessageToDict` byte for byte
//!   (`goldens/clouddrive/json_mapping.json`).
//! - scraper: what html5ever and `selectors` find on the pages the tests carry, against what
//!   pyquery/lxml found (`goldens/html/selectors.json`); every difference is listed by name.
//! - wreq: the impersonating client builds with a Chrome emulation and makes a request to a
//!   local listener (the fingerprint comparison is `tools/probes/tls_fingerprint.py`).
//!
//! No request leaves the machine. No application code is ported here.

use embyx_core::golden;
use embyx_core::pyjson::{self, DumpOptions, Value};

fn sorted(value: &Value) -> String {
    let options = DumpOptions {
        sort_keys: true,
        ..DumpOptions::python_default()
    };
    pyjson::dumps(value, &options).expect("dumps")
}

mod grpc {
    use embyx_core::pyjson::{Dict, Value};
    use embyx_core::scenario::Recorder;
    use tonic::metadata::MetadataValue;
    use tonic::{Code, Request};

    use crate::clouddrive::fake::FakeCloudDrive;
    use crate::clouddrive::proto::cloud_drive_file_srv_client::CloudDriveFileSrvClient;
    use crate::clouddrive::proto::{ListSubFileRequest, MoveFileRequest, move_file_request::ConflictPolicy};

    fn seed() -> Value {
        let mut file = Dict::new();
        file.insert("path", Value::from("/115/in/a.txt"));
        file.insert("size", Value::Int(8));
        let mut seed = Dict::new();
        seed.insert("directories", Value::List(vec![Value::from("/115/out")]));
        seed.insert("files", Value::List(vec![Value::Dict(file)]));
        Value::Dict(seed)
    }

    fn authorized<T>(message: T) -> Request<T> {
        // client.py `_metadata`: [('authorization', f'Bearer {api_token}')].
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer probe-token").expect("ascii"),
        );
        request
    }

    #[tokio::test]
    async fn the_generated_client_talks_to_the_fake_over_a_socket() {
        let recorder = Recorder::new();
        let served = FakeCloudDrive::new(&seed(), recorder.clone()).serve().await;
        // `secure=False`: plain HTTP/2 to host:port.
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", served.address))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut client = CloudDriveFileSrvClient::new(channel);

        let info = client
            .get_system_info(Request::new(()))
            .await
            .expect("GetSystemInfo")
            .into_inner();
        assert!(info.is_login && info.system_ready);
        // CloudDriveSystemInfo has no version field: Python's `check()` reports version ''.
        assert_eq!(info.user_name, "fake");

        let moved = client
            .move_file(authorized(MoveFileRequest {
                the_file_paths: vec!["/115/in/a.txt".to_owned()],
                dest_path: "/115/out".to_owned(),
                conflict_policy: Some(ConflictPolicy::Skip.into()),
                ..Default::default()
            }))
            .await
            .expect("MoveFile")
            .into_inner();
        assert!(moved.success);
        assert_eq!(moved.result_file_paths, ["/115/out/a.txt"]);

        let mut stream = client
            .get_sub_files(authorized(ListSubFileRequest {
                path: "/115/out".to_owned(),
                ..Default::default()
            }))
            .await
            .expect("GetSubFiles")
            .into_inner();
        let reply = stream.message().await.expect("reply").expect("one reply");
        assert_eq!(reply.sub_files[0].full_path_name, "/115/out/a.txt");
        assert_eq!(reply.sub_files[0].size, 8);
        assert!(stream.message().await.expect("end").is_none());

        // client.py turns NOT_FOUND into FileNotFoundError(details) and INVALID_ARGUMENT
        // "can't open a file as directory" into NotADirectoryError; both reach tonic intact,
        // also on a server stream where the error arrives with the response headers.
        let missing = client
            .get_sub_files(authorized(ListSubFileRequest {
                path: "/nope".to_owned(),
                ..Default::default()
            }))
            .await
            .expect_err("missing");
        assert_eq!(missing.code(), Code::NotFound);
        assert_eq!(missing.message(), "path not found: /nope");
        let file = client
            .get_sub_files(authorized(ListSubFileRequest {
                path: "/115/out/a.txt".to_owned(),
                ..Default::default()
            }))
            .await
            .expect_err("a file");
        assert_eq!(file.code(), Code::InvalidArgument);
        assert!(file.message().contains("can't open a file as directory"));

        // The rest of the 14 RPCs (WP3): an empty copy-task list from a fresh fake.
        let copies = client
            .get_copy_tasks(authorized(()))
            .await
            .expect("GetCopyTasks")
            .into_inner();
        assert!(copies.copy_tasks.is_empty());

        let methods: Vec<String> = recorder
            .calls()
            .iter()
            .map(|call| call.get("method").and_then(Value::as_str).expect("method").to_owned())
            .collect();
        assert_eq!(
            methods,
            [
                "GetSystemInfo",
                "MoveFile",
                "GetSubFiles",
                "GetSubFiles",
                "GetSubFiles",
                "GetCopyTasks"
            ]
        );
        drop(served);
    }

    #[test]
    fn status_details_carry_utf8() {
        // tonic percent-encodes grpc-message on the wire and decodes it back, so a Chinese
        // detail from CloudDrive reaches the client as written.
        let status = tonic::Status::new(Code::AlreadyExists, "任务已存在");
        let response: http::Response<tonic::body::Body> = status.clone().into_http();
        let header = response
            .headers()
            .get("grpc-message")
            .expect("grpc-message")
            .to_str()
            .expect("ascii");
        assert_eq!(header, "%E4%BB%BB%E5%8A%A1%E5%B7%B2%E5%AD%98%E5%9C%A8");
        let back = tonic::Status::from_header_map(response.headers()).expect("status");
        assert_eq!(back.message(), "任务已存在");
    }
}

#[test]
fn the_fake_records_what_message_to_dict_records() {
    let document = golden::load("clouddrive/json_mapping.json");
    let cases = golden::cases(&document);
    assert_eq!(cases.len(), 12);
    for case in cases {
        let name = golden::string(golden::get(case, "message"));
        let bytes = hex::decode(golden::string(golden::get(case, "bytes"))).expect("hex");
        let have = crate::clouddrive::fake::bytes_to_json(&name, &bytes);
        assert_eq!(sorted(&have), sorted(golden::get(case, "json")), "{name}");
    }
}

mod html {
    //! scraper (html5ever + selectors) against pyquery/lxml.

    use std::collections::BTreeSet;

    use embyx_core::golden;
    use embyx_core::pyjson::{Key, Value};
    use scraper::{ElementRef, Html, Selector};

    /// Tag, sorted attributes, `text_content`.
    type Match = (String, Vec<(String, String)>, String);

    fn selector(text: &str) -> Selector {
        Selector::parse(text).unwrap_or_else(|error| panic!("selector {text:?}: {error}"))
    }

    /// (tag, attributes, `text_content`) of a match: what the golden records besides pyquery's
    /// whitespace-squashed `.text()`, which the port computes itself. Attributes are sorted:
    /// scraper keeps them in a sorted map, and the code only reads them by name.
    fn describe(element: ElementRef<'_>) -> Match {
        let value = element.value();
        let mut attrs: Vec<_> = value
            .attrs()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        attrs.sort();
        (value.name().to_owned(), attrs, element.text().collect())
    }

    fn golden_matches(matches: &Value) -> Vec<Match> {
        golden::list(matches)
            .iter()
            .map(|item| {
                let mut attrs: Vec<_> = golden::list(golden::get(item, "attrs"))
                    .iter()
                    .map(|pair| {
                        let pair = golden::list(pair);
                        (golden::string(&pair[0]), golden::string(&pair[1]))
                    })
                    .collect();
                attrs.sort();
                (
                    golden::string(golden::get(item, "tag")),
                    attrs,
                    golden::string(golden::get(item, "text_content")),
                )
            })
            .collect()
    }

    fn key_text(key: &Key) -> String {
        match key {
            Key::Str(text) => text.clone(),
            other => panic!("selector keys are strings, not {other:?}"),
        }
    }

    fn select_all<'a>(scope: ElementRef<'a>, text: &str) -> Vec<ElementRef<'a>> {
        scope.select(&selector(text)).collect()
    }

    /// Short labels of the cases (`tests/test_javbus.py:20`, `synthetic: ...`), each with the
    /// scopes where scraper differs: `doc:<selector>`, `rows:<n>` (a different number of
    /// rows), `row:<selector>`, `anchors:<n>`, `anchor:<selector>`.
    fn compare(case: &Value) -> BTreeSet<String> {
        let mut differences = BTreeSet::new();
        let html = golden::string(golden::get(case, "html"));
        let document = Html::parse_document(&html);
        let root = document.root_element();
        let documents = golden::get(case, "document").as_dict().expect("document");
        for (key, matches) in documents.iter() {
            let key = key_text(key);
            let have: Vec<_> = select_all(root, &key).into_iter().map(describe).collect();
            if have != golden_matches(matches) {
                differences.insert(format!("doc:{key}"));
            }
        }
        let rows = select_all(root, "tr");
        let want_rows = golden::list(golden::get(case, "rows"));
        if rows.len() == want_rows.len() {
            for (row, want) in rows.iter().zip(want_rows) {
                for (key, matches) in want.as_dict().expect("row").iter() {
                    let key = key_text(key);
                    let have: Vec<_> = select_all(*row, &key).into_iter().map(describe).collect();
                    if have != golden_matches(matches) {
                        differences.insert(format!("row:{key}"));
                    }
                }
            }
        } else {
            differences.insert(format!("rows:{}!={}", rows.len(), want_rows.len()));
        }
        let anchors = select_all(root, "a[href]");
        let want_anchors = golden::list(golden::get(case, "anchors"));
        if anchors.len() == want_anchors.len() {
            for (anchor, want) in anchors.iter().zip(want_anchors) {
                for (key, matches) in want.as_dict().expect("anchor").iter() {
                    let key = key_text(key);
                    let have: Vec<_> = select_all(*anchor, &key).into_iter().map(describe).collect();
                    if have != golden_matches(matches) {
                        differences.insert(format!("anchor:{key}"));
                    }
                }
            }
        } else {
            differences.insert(format!("anchors:{}!={}", anchors.len(), want_anchors.len()));
        }
        differences
    }

    /// The documented differences: `(case, scope)`. Why each happens is in
    /// docs/rewrite/API-COVERAGE.md ("scraper"); WP2 (JavBus and the magnet sources)
    /// decides how the port meets the baseline there.
    const KNOWN: &[(&str, &str)] = &[
        // html5ever inserts the implied `tbody`; lxml does not, so `table tbody tr`
        // (`rss_magnet.py`) finds rows in scraper that pyquery never saw.
        ("synthetic: a paragraph left open before a table", "doc:table tbody tr"),
        (
            "synthetic: magnet table without tbody, as a feed item may carry it",
            "doc:table tbody tr",
        ),
        (
            "synthetic: unclosed cells and an unquoted attribute",
            "doc:table tbody tr",
        ),
        // pyquery parses well-formed markup as XML, where `A` and `TR` are not `a` and `tr`;
        // html5ever lower-cases every tag.
        ("synthetic: upper-case tags in well-formed markup", "anchors:2!=1"),
        ("synthetic: upper-case tags in well-formed markup", "doc:a[href]"),
        ("synthetic: upper-case tags in well-formed markup", "doc:table tbody tr"),
        ("synthetic: upper-case tags in well-formed markup", "doc:tr"),
        ("synthetic: upper-case tags in well-formed markup", "rows:1!=0"),
        // JavBus's magnet rows arrive as bare `<tr>` elements; html5ever drops a `tr` outside
        // a table, lxml.html keeps it (see `bare_rows_need_a_table_context`).
        ("tests/test_javbus.py:20", "doc:tr"),
        ("tests/test_javbus.py:20", "rows:0!=2"),
        // pyquery raises ParserError on a document with no element; html5ever returns an empty one.
        ("synthetic: an end tag and no element", "error"),
    ];

    #[test]
    fn bare_rows_need_a_table_context() {
        let document = golden::load("html/selectors.json");
        let case = golden::cases(&document)
            .iter()
            .find(|case| golden::string(golden::get(case, "name")) == "tests/test_javbus.py:20")
            .expect("the JavBus magnet rows fixture");
        let html = golden::string(golden::get(case, "html"));
        let want = golden::list(golden::get(golden::get(case, "document"), "tr")).len();
        assert_eq!(want, 2);
        assert_eq!(Html::parse_document(&html).select(&selector("tr")).count(), 0);
        // Parsing the rows inside a table keeps them.
        let wrapped = Html::parse_document(&format!("<table>{html}</table>"));
        let rows: Vec<_> = wrapped.select(&selector("tr")).collect();
        assert_eq!(rows.len(), want);
        let cells: Vec<String> = rows[0]
            .select(&selector("td:first-child a"))
            .map(|link| link.text().collect::<String>().trim().to_owned())
            .collect();
        assert_eq!(cells, ["release1"]);
    }

    #[test]
    fn scraper_finds_what_pyquery_found() {
        let document = golden::load("html/selectors.json");
        let mut differences = BTreeSet::new();
        for case in golden::cases(&document) {
            let name = golden::string(golden::get(case, "name"));
            let label = name.split(" (").next().unwrap_or(&name).to_owned();
            if case.get("error").is_some() {
                // pyquery raised (an empty document); html5ever never refuses a document.
                let html = golden::string(golden::get(case, "html"));
                let _ = Html::parse_document(&html);
                differences.insert((label, "error".to_owned()));
                continue;
            }
            for scope in compare(case) {
                differences.insert((label.clone(), scope));
            }
        }
        let known: BTreeSet<(String, String)> = KNOWN
            .iter()
            .map(|(case, scope)| ((*case).to_owned(), (*scope).to_owned()))
            .collect();
        assert_eq!(differences, known);
    }
}

mod impersonation {
    //! AVBase is fetched with `curl_cffi` impersonating Chrome (`clients/avbase.py`). wreq
    //! (BoringSSL) is the candidate; this checks that it builds, takes an emulation profile
    //! and talks to a local TLS listener with HTTP/2. Whether its ClientHello and HTTP/2
    //! SETTINGS match `curl_cffi`'s are measured by `tools/probes/tls_fingerprint.py`.

    use std::sync::Arc;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test]
    async fn wreq_with_a_chrome_emulation_reaches_a_local_listener() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("certificate");
        let certificate = certified.cert.der().clone();
        let key = rustls::pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der()).expect("key");
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .expect("server config");
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let mut tls = acceptor.accept(socket).await.expect("handshake");
            let alpn = tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
            // Read the HTTP/2 preface and the first frames, then hang up.
            let mut buffer = vec![0_u8; 4096];
            let read = tls.read(&mut buffer).await.unwrap_or(0);
            let _ = tls.shutdown().await;
            (alpn, buffer[..read].to_vec())
        });

        let client = wreq::Client::builder()
            .emulation(wreq_util::Emulation::Chrome149)
            .tls_cert_verification(false)
            .build()
            .expect("client");
        // The server hangs up after the preface: the request fails, the handshake is the point.
        let _ = client.get(format!("https://localhost:{port}/")).send().await;
        let (alpn, bytes) = server.await.expect("server task");
        assert_eq!(alpn.as_deref(), Some(&b"h2"[..]), "Chrome emulation negotiates HTTP/2");
        assert!(
            bytes.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
            "an HTTP/2 preface"
        );
    }
}
