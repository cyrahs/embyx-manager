//! `static_files` against Python: the goldens of `tools/goldens/areas/api.py`, and the
//! lookup rules on a real directory.

use std::path::Path;

use embyx_core::golden;
use embyx_core::pyjson::Value;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::BodyExt as _;

use super::{
    RangeOutcome, SpaStaticFiles, Stat, formatdate, get_path, insert_trailing_slash, is_page_path, parse_range_header,
    parsedate, realpath, stat_headers,
};
use crate::http::RequestHead;

#[test]
fn static_paths_match_python() {
    let document = golden::load("api/static_paths.json");
    for case in golden::cases(&document) {
        let route_path = golden::string(golden::get(case, "route_path"));
        let path = get_path(&route_path);
        assert_eq!(path, golden::string(golden::get(case, "path")), "{route_path:?}");
        assert_eq!(
            Value::Bool(is_page_path(&path)),
            *golden::get(case, "is_page_path"),
            "{route_path:?}"
        );
    }
}

#[test]
fn range_headers_parse_as_python_parses_them() {
    let document = golden::load("api/ranges.json");
    for case in golden::cases(&document) {
        let header = golden::string(golden::get(case, "header"));
        let size = golden::int(golden::get(case, "size"));
        let outcome = parse_range_header(&header, size);
        let shown = header.chars().take(60).collect::<String>();
        if let Some(ranges) = case.get("ranges") {
            let expected: Vec<(i128, i128)> = golden::list(ranges)
                .iter()
                .map(|pair| {
                    let pair = golden::list(pair);
                    (golden::int(&pair[0]), golden::int(&pair[1]))
                })
                .collect();
            // Past i128 Python keeps the exact integer; only its order matters here.
            let saturated = |pairs: Vec<(i128, i128)>| -> Vec<(i128, i128)> {
                pairs
                    .into_iter()
                    .map(|(start, end)| (start.min(10_i128.pow(36)), end.min(10_i128.pow(36))))
                    .collect()
            };
            assert_eq!(outcome, RangeOutcome::Ranges(saturated(expected)), "{shown} / {size}");
        } else if let Some(content) = case.get("malformed") {
            assert_eq!(
                outcome,
                RangeOutcome::Malformed(Box::leak(golden::string(content).into_boxed_str())),
                "{shown} / {size}"
            );
        } else {
            assert_eq!(outcome, RangeOutcome::NotSatisfiable, "{shown} / {size}");
        }
    }
}

#[test]
fn dates_parse_as_email_utils_parses_them() {
    let document = golden::load("api/parsedate.json");
    for case in golden::cases(&document) {
        let text = golden::string(golden::get(case, "text"));
        let expected = match golden::get(case, "parsed") {
            Value::Null => None,
            parsed => {
                let fields = golden::list(parsed);
                assert_eq!(
                    fields[6..].iter().map(golden::int).collect::<Vec<_>>(),
                    [0, 1, -1],
                    "{text:?}"
                );
                Some(std::array::from_fn::<i128, 6, _>(|index| golden::int(&fields[index])))
            }
        };
        assert_eq!(parsedate(&text), expected, "{text:?}");
    }
}

#[test]
fn validators_match_file_response() {
    let document = golden::load("api/file_headers.json");
    for case in golden::cases(&document) {
        let nanos = golden::int(golden::get(case, "mtime_ns"));
        let stat = Stat {
            is_file: true,
            is_dir: false,
            size: u64::try_from(golden::int(golden::get(case, "size"))).expect("a size"),
            mtime_sec: i64::try_from(nanos / 1_000_000_000).expect("seconds"),
            mtime_nsec: i64::try_from(nanos % 1_000_000_000).expect("nanoseconds"),
        };
        assert_eq!(
            embyx_core::pyjson::float_repr(stat.mtime()),
            golden::string(golden::get(case, "st_mtime")),
            "{nanos}"
        );
        let (length, last_modified, etag) = stat_headers(stat);
        assert_eq!(length, stat.size.to_string());
        assert_eq!(
            last_modified,
            golden::string(golden::get(case, "last_modified")),
            "{nanos}"
        );
        assert_eq!(etag, golden::string(golden::get(case, "etag")), "{nanos}");
    }
    assert_eq!(formatdate(-0.5), "Wed, 31 Dec 1969 23:59:59 GMT");
}

#[test]
fn a_trailing_slash_goes_where_the_path_ends() {
    assert_eq!(
        insert_trailing_slash("http://testserver/guide"),
        "http://testserver/guide/"
    );
    assert_eq!(
        insert_trailing_slash("http://testserver/guide?x=1"),
        "http://testserver/guide/?x=1"
    );
    assert_eq!(insert_trailing_slash("/guide#top"), "/guide/#top");
}

fn request(method: Method, path: &str) -> RequestHead {
    RequestHead {
        method,
        path: path.to_owned(),
        query: String::new(),
        headers: HeaderMap::new(),
        server: None,
    }
}

async fn served(files: &SpaStaticFiles, path: &str) -> (StatusCode, String) {
    let response = files.serve(&request(Method::GET, path)).await.expect("an answer");
    let status = response.status();
    let body = response.into_body().collect().await.expect("a body").to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn write(root: &Path, relative: &str, text: &str) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("mkdir");
    std::fs::write(path, text).expect("write");
}

#[tokio::test]
async fn symlinks_resolve_and_nothing_outside_the_root_is_served() {
    let outside = tempfile::tempdir().expect("tempdir");
    write(outside.path(), "secret.txt", "secret");
    let root = tempfile::tempdir().expect("tempdir");
    write(root.path(), "index.html", "index");
    write(root.path(), "real/inside.txt", "inside");
    std::os::unix::fs::symlink(root.path().join("real"), root.path().join("alias")).expect("symlink");
    std::os::unix::fs::symlink(outside.path().join("secret.txt"), root.path().join("escape.txt")).expect("symlink");
    std::os::unix::fs::symlink("loop", root.path().join("loop")).expect("symlink");
    let files = SpaStaticFiles::new(root.path().to_path_buf());

    assert_eq!(
        served(&files, "/alias/inside.txt").await,
        (StatusCode::OK, "inside".to_owned())
    );
    // The link points outside the root: not served, and not a page path either.
    assert_eq!(served(&files, "/escape.txt").await.0, StatusCode::NOT_FOUND);
    // A page path that finds nothing falls back to the index.
    assert_eq!(served(&files, "/escape").await, (StatusCode::OK, "index".to_owned()));
    // A symlink loop is an `OSError` (ELOOP) nothing handles.
    assert!(files.serve(&request(Method::GET, "/loop.txt")).await.is_ok());
    assert!(files.serve(&request(Method::GET, "/loop")).await.is_err());
    // NUL and a name too long for the file system are 404s; the latter is still a page
    // path, so it gets the index.
    assert_eq!(served(&files, "/a\0b.txt").await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        served(&files, &format!("/{}.txt", "x".repeat(300))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(served(&files, &format!("/{}", "x".repeat(300))).await.0, StatusCode::OK);

    let resolved = realpath(&root.path().join("alias/../alias/inside.txt").to_string_lossy());
    assert_eq!(
        resolved,
        std::fs::canonicalize(root.path().join("real/inside.txt"))
            .expect("canonical")
            .to_string_lossy()
    );
    assert!(realpath("/no/such/./dir/../file").ends_with("/no/such/file"));
}

#[tokio::test]
async fn a_missing_root_is_a_server_error_and_other_methods_are_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    write(root.path(), "index.html", "index");
    let files = SpaStaticFiles::new(root.path().join("gone"));
    assert!(files.serve(&request(Method::GET, "/")).await.is_err());

    let files = SpaStaticFiles::new(root.path().to_path_buf());
    assert_eq!(served(&files, "/").await, (StatusCode::OK, "index".to_owned()));
    // Methods other than GET and HEAD: 405 without `Allow`.
    let response = files
        .serve(&request(Method::POST, "/index.html"))
        .await
        .expect("an answer");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(response.headers().get("allow").is_none());
    // A 404.html is served with its status, and a page path still gets the index.
    write(root.path(), "404.html", "missing");
    assert_eq!(
        served(&files, "/x.js").await,
        (StatusCode::NOT_FOUND, "missing".to_owned())
    );
    assert_eq!(served(&files, "/x").await, (StatusCode::OK, "index".to_owned()));
}
