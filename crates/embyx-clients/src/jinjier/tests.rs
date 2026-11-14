//! jinjier.art source: which lists and entries survive, and how the archive is fetched.

use std::io::Write as _;
use std::time::Duration;

use embyx_core::avid::AvidParser;

use super::fixtures::{ROWS, make_database, serialize};
use super::*;
use crate::net::fakeweb::{FakeWeb, Reply, Seen};

fn avid_of() -> impl Fn(&str) -> String {
    let parser = AvidParser::default();
    move |title: &str| parser.get_avid(title)
}

fn by_key(lists: Vec<RankedList>) -> HashMap<String, RankedList> {
    lists.into_iter().map(|ranked| (ranked.key.clone(), ranked)).collect()
}

// py: tests/test_jinjier.py::test_parse_keeps_only_the_playlist_families_and_drops_uncensored_entries
#[test]
fn parse_keeps_only_the_playlist_families_and_drops_uncensored_entries() {
    let parsed = parse_lists(&make_database(ROWS), &avid_of()).expect("lists");
    let keys: Vec<&str> = parsed.iter().map(|ranked| ranked.key.as_str()).collect();
    let lists = by_key(parsed.clone());
    let mut sorted = keys.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        [
            "k2024",
            "k4:第四届JAV金鸡儿奖 最佳故事片 提名",
            "k4:第四届JAV金鸡儿奖 最佳故事片 获奖",
            "k5",
            "k7",
        ]
    );
    // The lists come in the query's order (kind, then note), as Python's dict keeps them.
    assert_eq!(
        keys,
        [
            "k4:第四届JAV金鸡儿奖 最佳故事片 提名",
            "k4:第四届JAV金鸡儿奖 最佳故事片 获奖",
            "k5",
            "k7",
            "k2024",
        ]
    );
    // Same code twice in a list keeps its first position; later entries close the gap.
    let k7: Vec<(usize, &str)> = lists["k7"].entries.iter().map(|e| (e.rank, e.avid.as_str())).collect();
    assert_eq!(k7, [(1, "SSNI-497"), (2, "IPX-811"), (3, "ABP-984")]);
    assert_eq!(lists["k7"].name(), "JavDB 有码 TOP250");
    // Western, FC2, date codes, a label from the uncensored set, an uncensored brand, and a
    // Blu-ray duplicate all go; the survivors are renumbered.
    let k2024: Vec<&str> = lists["k2024"].entries.iter().map(|e| e.avid.as_str()).collect();
    assert_eq!(k2024, ["MIAB-317", "SONE-308", "TEK-099"]);
    let ranks: Vec<usize> = lists["k2024"].entries.iter().map(|e| e.rank).collect();
    assert_eq!(ranks, [1, 2, 3]);
    // The people rows of an awards category vanish with it: no empty lists.
    assert_eq!(
        lists["k4:第四届JAV金鸡儿奖 最佳故事片 提名"].entries,
        [
            RankedEntry {
                rank: 1,
                avid: "MOON-018".to_owned(),
                title: "いつも濡れすぎて".to_owned(),
            },
            RankedEntry {
                rank: 2,
                avid: "START-073".to_owned(),
                title: "MINAMO ZOMBIE".to_owned(),
            },
        ]
    );
    assert_eq!(lists["k5"].entries[1].title, "オフィスレディの湿ったパンスト 桜木凛");
}

// py: tests/test_jinjier.py::test_parse_reads_the_uncensored_set_through_the_parser_normalization
#[test]
fn parse_reads_the_uncensored_set_through_the_parser_normalization() {
    // SERO-0127 in the uncensored list and SERO-127 in the year list are the same title once
    // both spellings pass through the parser.
    let parser = AvidParser::default();
    assert_eq!(parser.get_avid("SERO-0127"), "SERO-127");
    assert_eq!(parser.get_avid("SERO-127"), "SERO-127");
    let lists = by_key(parse_lists(&make_database(ROWS), &avid_of()).expect("lists"));
    assert!(!lists["k2024"].entries.iter().any(|e| e.avid == "SERO-127"));
}

// py: tests/test_jinjier.py::test_parse_rejects_bytes_that_are_not_a_ranking_database
#[test]
fn parse_rejects_bytes_that_are_not_a_ranking_database() {
    let error = parse_lists(b"not sqlite at all", &avid_of()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "not a readable ranking database: file is not a database"
    );

    let empty = rusqlite::Connection::open_in_memory().expect("open");
    empty.execute("CREATE TABLE other (x)", []).expect("create");
    let error = parse_lists(&serialize(&empty), &avid_of()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "not a readable ranking database: no such table: ranks"
    );

    // D30: zero bytes are a `MemoryError` in Python, a `JinjierError` here.
    let error = parse_lists(b"", &avid_of()).unwrap_err();
    assert!(error.0.starts_with("not a readable ranking database: "), "{error}");
}

/// A `ranks` table of untyped columns holding `rows` (kind, number, name, note).
fn untyped(rows: &str) -> Vec<u8> {
    let connection = rusqlite::Connection::open_in_memory().expect("open");
    connection
        .execute_batch(&format!(
            "CREATE TABLE ranks (id INTEGER PRIMARY KEY, kind, number, name, note);\
             INSERT INTO ranks (kind, number, name, note) VALUES {rows};"
        ))
        .expect("rows");
    serialize(&connection)
}

#[test]
fn notes_and_names_are_checked_only_where_python_reads_them() {
    // Python reads a row's name and note only when it uses the row (each result below was
    // checked against the baseline's `parse_lists`).
    let lists = parse_lists(
        &untyped("(7, 1, 'ABP-001 title', 'JavDB'), (1, 1, NULL, NULL)"),
        &avid_of(),
    )
    .unwrap();
    assert_eq!(lists.len(), 1);
    assert_eq!((lists[0].key.as_str(), lists[0].note.as_str()), ("k7", "JavDB"));
    assert_eq!(lists[0].entries[0].avid, "ABP-001");
    // A list left empty never needs its note.
    assert_eq!(
        parse_lists(&untyped("(7, 1, 'nothing', NULL)"), &avid_of()),
        Ok(Vec::new())
    );
}

#[test]
fn rows_of_other_types_are_not_a_ranking_database() {
    let refused = |rows: &str| {
        let error = parse_lists(&untyped(rows), &avid_of()).unwrap_err();
        assert_eq!(
            error.0, "not a readable ranking database: unexpected column types in ranks",
            "{rows}"
        );
    };
    // Python: `AttributeError` on `name.strip()` (a used row, or an uncensored-list row).
    refused("(7, 1, NULL, 'JavDB')");
    refused("(8, 1, NULL, 'x')");
    // Python: `TypeError` from `2007 <= '7'`.
    refused("('7', 1, 'ABP-001 t', 'n')");
    // D30: Python returns these lists with a note of `None` or `5`, which the `playlists`
    // table (`note TEXT NOT NULL`) then refuses; here the import fails at once.
    refused("(7, 1, 'ABP-001 title', 'JavDB'), (7, 2, 'no code here', NULL)");
    refused("(4, 1, 'ABP-001 t', 5)");
}

// py: tests/test_jinjier.py::test_excluded_codes cases=10
#[test]
fn excluded_codes() {
    let cases = [
        ("SSNI-497", false),
        ("FC2-4025269", true),
        ("010115_001", true),
        ("092415-159", true),
        ("HEYZO-1031", true),
        ("LAFBD-041", true),
        ("SKY-247", true),
        ("SKYHD-084", true),
        ("BT-123", true),
        ("BTH-123", false),
    ];
    for (avid, excluded) in cases {
        assert_eq!(is_excluded_code(avid), excluded, "{avid}");
    }
    // `$` also matches before a trailing newline, as in Python.
    assert!(is_excluded_code("010115_001\n"));
}

// py: tests/test_jinjier.py::test_list_key_only_joins_the_note_for_awards
#[test]
fn list_key_only_joins_the_note_for_awards() {
    assert_eq!(list_key(7, "JavDB 有码 TOP250"), "k7");
    assert_eq!(list_key(2024, "JavDB 2024 TOP250"), "k2024");
    assert_eq!(
        list_key(4, "第四届JAV金鸡儿奖 最佳导演 提名"),
        "k4:第四届JAV金鸡儿奖 最佳导演 提名"
    );
}

// -- HTTP ---------------------------------------------------------------------

const SQL_PAGE: &str = "<html><script>const wasmFile=\"x\";let defaultSQL=\"...\";\
    dbFile=new URLSearchParams(location.search).get(\"v\");\
    \"2023\"==dbFile?(dbFile=\"https://cdn/2023.gif\"):dbFile=\"20260112.gif\";</script></html>";

fn archive(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = zip::ZipWriter::new(&mut buffer);
        let options = zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        for (name, data) in members {
            writer.start_file(*name, options).expect("member");
            writer.write_all(data).expect("write");
        }
        writer.finish().expect("finish");
    }
    buffer.into_inner()
}

fn options(web: &FakeWeb) -> ClientOptions {
    ClientOptions {
        proxy: Some(web.proxy.clone()),
        ..JinjierClient::client_options(Duration::from_secs(5))
    }
}

async fn make_client(routes: Vec<(&'static str, Reply)>) -> (JinjierClient, FakeWeb) {
    let web = FakeWeb::start(move |seen: &Seen| {
        routes
            .iter()
            .find(|(url, _)| *url == seen.url)
            .map_or_else(|| Reply::status(404), |(_, reply)| reply.clone())
    })
    .await;
    let client = JinjierClient::with_client("http://jinjier.test/sql", &options(&web)).expect("client");
    (client, web)
}

// py: tests/test_jinjier.py::test_discover_reads_the_current_database_name_from_the_query_page
#[tokio::test]
async fn discover_reads_the_current_database_name_from_the_query_page() {
    let (client, web) = make_client(vec![("http://jinjier.test/sql", Reply::bytes(200, SQL_PAGE))]).await;
    assert_eq!(client.discover_database_name().await.expect("name"), "20260112");
    assert_eq!(web.urls(), ["http://jinjier.test/sql"]);
}

// py: tests/test_jinjier.py::test_discover_fails_loudly_when_the_page_changed_shape
#[tokio::test]
async fn discover_fails_loudly_when_the_page_changed_shape() {
    let (client, _web) = make_client(vec![(
        "http://jinjier.test/sql",
        Reply::bytes(200, "<html>redesigned</html>"),
    )])
    .await;
    let error = client.discover_database_name().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "http://jinjier.test/sql: no database name in the page"
    );
}

// py: tests/test_jinjier.py::test_download_unpacks_the_sqlite_member_from_the_root_level_archive
#[tokio::test]
async fn download_unpacks_the_sqlite_member_from_the_root_level_archive() {
    let database = make_database(ROWS);
    let body = archive(&[("jinjier.sqlite3", &database)]);
    let (client, web) = make_client(vec![("http://jinjier.test/20260112.gif", Reply::bytes(200, body))]).await;
    assert_eq!(client.download_database("20260112").await.expect("database"), database);
    // The archive lives at the origin root, not under the query page's path.
    assert_eq!(web.urls(), ["http://jinjier.test/20260112.gif"]);
}

// py: tests/test_jinjier.py::test_download_rejects_non_archives_and_archives_without_a_database
#[tokio::test]
async fn download_rejects_non_archives_and_archives_without_a_database() {
    let (client, _web) = make_client(vec![
        ("http://jinjier.test/a.gif", Reply::bytes(200, b"GIF89a...".to_vec())),
        (
            "http://jinjier.test/b.gif",
            Reply::bytes(200, archive(&[("readme.txt", b"hi")])),
        ),
        ("http://jinjier.test/c.gif", Reply::status(503)),
        (
            "http://jinjier.test/d.gif",
            Reply::bytes(200, b"PK\x03\x04 but nothing after".to_vec()),
        ),
    ])
    .await;
    let message = |result: Result<Vec<u8>, JinjierError>| result.unwrap_err().to_string();
    assert_eq!(
        message(client.download_database("a").await),
        "http://jinjier.test/a.gif: not a zip archive"
    );
    assert_eq!(
        message(client.download_database("b").await),
        "http://jinjier.test/b.gif: no .sqlite3 member in the archive"
    );
    assert_eq!(
        message(client.download_database("c").await),
        "http://jinjier.test/c.gif: HTTP 503"
    );
    assert_eq!(
        message(client.download_database("d").await),
        "http://jinjier.test/d.gif: corrupt zip archive"
    );
}

// py: tests/test_jinjier.py::test_transport_failures_are_jinjier_errors
#[tokio::test]
async fn transport_failures_are_jinjier_errors() {
    let refused = JinjierClient::with_client(
        "http://jinjier.test/sql",
        &ClientOptions {
            proxy: Some("http://127.0.0.1:9".to_owned()),
            ..JinjierClient::client_options(Duration::from_secs(5))
        },
    )
    .expect("client");
    let error = refused.discover_database_name().await.unwrap_err();
    assert!(
        error.0.starts_with("http://jinjier.test/sql: ConnectError: "),
        "{error}"
    );

    // A hang-up, a body cut short and a stall are transport failures too.
    for (reply, class) in [
        (Reply::Close, "RemoteProtocolError"),
        (
            Reply::Truncated {
                status: 200,
                sent: b"dbFile=".to_vec(),
                length: 100,
            },
            "RemoteProtocolError",
        ),
        (Reply::Stall(Duration::from_secs(5)), "ReadTimeout"),
    ] {
        let web = FakeWeb::start(move |_: &Seen| reply.clone()).await;
        let client = JinjierClient::with_client(
            "http://jinjier.test/sql",
            &ClientOptions {
                timeout: Duration::from_millis(300),
                ..options(&web)
            },
        )
        .expect("client");
        let error = client.discover_database_name().await.unwrap_err();
        assert!(
            error.0.starts_with(&format!("http://jinjier.test/sql: {class}: ")),
            "{error}"
        );
    }
}

#[test]
fn the_origin_drops_the_query_page_path() {
    let client = JinjierClient::new("https://jinjier.art/tools/sql?x=1#top").expect("client");
    assert_eq!(client.database_url("20260112"), "https://jinjier.art/20260112.gif");
    assert!(matches!(
        JinjierClient::new("http://[::1/sql"),
        Err(JinjierBuildError::Url(_))
    ));
}

// `goldens/clients/jinjier.json`: which codes the source drops and how list kinds are keyed.
#[test]
fn codes_and_kinds_match_the_baseline() {
    let document = embyx_core::golden::load("clients/jinjier.json");
    let (mut codes, mut kinds) = (0, 0);
    for case in embyx_core::golden::cases(&document) {
        if let Some(code) = case.get("code") {
            let code = embyx_core::golden::string(code);
            let excluded = embyx_core::golden::get(case, "excluded");
            assert_eq!(
                is_excluded_code(&code),
                matches!(excluded, embyx_core::pyjson::Value::Bool(true)),
                "{code:?}"
            );
            codes += 1;
        } else {
            let kind = i64::try_from(embyx_core::golden::int(embyx_core::golden::get(case, "kind"))).unwrap();
            let playlist = embyx_core::golden::get(case, "playlist");
            assert_eq!(
                is_playlist_kind(kind),
                matches!(playlist, embyx_core::pyjson::Value::Bool(true)),
                "{kind}"
            );
            assert_eq!(
                list_key(kind, "n"),
                embyx_core::golden::string(embyx_core::golden::get(case, "key")),
                "{kind}"
            );
            kinds += 1;
        }
    }
    assert_eq!((codes, kinds), (19, 13));
}

#[test]
fn the_database_name_pattern_spaces_as_python_does() {
    // Python's `\s` takes U+001C to U+001F as well.
    let found = DB_FILE_RE
        .captures("dbFile\u{1c}=\u{1f}\"20260112.gif\"")
        .expect("a match");
    assert_eq!(&found[1], "20260112");
}
