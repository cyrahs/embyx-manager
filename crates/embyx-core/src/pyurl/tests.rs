use super::{quote, unquote, urljoin, urlsplit, urlunsplit};
use crate::golden;
use crate::pyjson::Value;
use crate::pypath;

#[test]
fn unquote_matches_python() {
    let document = golden::load("pyweb/unquote.json");
    for case in golden::cases(&document) {
        let text = golden::string(golden::get(case, "text"));
        assert_eq!(unquote(&text), golden::string(golden::get(case, "result")), "{text:?}");
    }
}

#[test]
fn urljoin_matches_python() {
    let document = golden::load("pyweb/urljoin.json");
    for case in golden::cases(&document) {
        let base = golden::string(golden::get(case, "base"));
        let url = golden::string(golden::get(case, "url"));
        let outcome = urljoin(&base, &url);
        match (case.get("result"), case.get("error")) {
            (Some(result), None) => assert_eq!(
                outcome.as_deref(),
                Ok(golden::string(result).as_str()),
                "{base:?} {url:?}"
            ),
            (None, Some(error)) => assert_eq!(
                outcome.map_err(|error| error.0),
                Err(golden::string(error)),
                "{base:?} {url:?}"
            ),
            _ => panic!("malformed case {case:?}"),
        }
    }
}

#[test]
fn urlsplit_matches_python() {
    let document = golden::load("pyweb/urlsplit.json");
    for case in golden::cases(&document) {
        let url = golden::string(golden::get(case, "url"));
        let parts = urlsplit(&url, "").expect("splits");
        let expected: Vec<String> = golden::list(golden::get(case, "parts"))
            .iter()
            .map(golden::string)
            .collect();
        assert_eq!(
            vec![parts.scheme, parts.netloc, parts.path, parts.query, parts.fragment],
            expected,
            "{url:?}"
        );
    }
}

// WP5's `urls` goldens: `SplitResult.hostname`, the round trip through `urlunsplit`,
// `quote` with and without `safe`, and the `PurePosixPath` parts Kemono and Jandan use.
#[test]
fn urlsplit_hostname_and_unsplit_match_python() {
    let document = golden::load("urls/urlsplit.json");
    for case in golden::cases(&document) {
        let url = golden::string(golden::get(case, "url"));
        let expected = golden::get(case, "result");
        let actual = match urlsplit(&url, "") {
            Ok(split) => Value::List(vec![
                Value::Str(split.scheme.clone()),
                Value::Str(split.netloc.clone()),
                Value::Str(split.path.clone()),
                Value::Str(split.query.clone()),
                Value::Str(split.fragment.clone()),
                split.hostname().map_or(Value::Null, Value::Str),
                Value::Str(urlunsplit(&split)),
            ]),
            Err(error) => Value::Str(format!("ValueError: {error}")),
        };
        assert_eq!(&actual, expected, "{url:?}");
    }
}

#[test]
fn quoting_matches_python() {
    let document = golden::load("urls/quote.json");
    for case in golden::cases(&document) {
        let text = golden::string(golden::get(case, "text"));
        assert_eq!(
            quote(&text, ""),
            golden::string(golden::get(case, "quote_no_safe")),
            "{text:?}"
        );
        assert_eq!(
            quote(&text, "/"),
            golden::string(golden::get(case, "quote")),
            "{text:?}"
        );
        assert_eq!(unquote(&text), golden::string(golden::get(case, "unquote")), "{text:?}");
    }
}

#[test]
fn path_parts_match_python() {
    let document = golden::load("urls/paths.json");
    for case in golden::cases(&document) {
        let path = golden::string(golden::get(case, "path"));
        assert_eq!(
            pypath::name(&path),
            golden::string(golden::get(case, "name")),
            "{path:?}"
        );
        assert_eq!(
            pypath::suffix(&path),
            golden::string(golden::get(case, "suffix")),
            "{path:?}"
        );
        assert_eq!(
            pypath::stem(&path),
            golden::string(golden::get(case, "stem")),
            "{path:?}"
        );
    }
}
