//! `posixpath` string functions: `normpath`, `split`, `dirname`, `basename`, `join`.
//!
//! Python origin: `posixpath` in `CPython` 3.13, as the CloudDrive wrapper
//! (`clients/clouddrive/aio.py`) and the scenario fake (`tools/scenarios/fakes/clouddrive.py`)
//! use it on CloudDrive API paths. Unlike pathlib ([`crate::pypath`]) these keep a trailing
//! slash where Python keeps it and resolve `..` lexically. `goldens/text/posixpath.json` pins
//! them against Python.

/// `posixpath.normpath(path)`.
pub fn normpath(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    // POSIX keeps exactly two leading slashes; three or more are one.
    let initial = if path.starts_with("//") && !path.starts_with("///") {
        2
    } else {
        usize::from(path.starts_with('/'))
    };
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part != ".." || (initial == 0 && parts.is_empty()) || parts.last() == Some(&"..") {
            parts.push(part);
        } else if !parts.is_empty() {
            parts.pop();
        }
    }
    let joined = format!("{}{}", "/".repeat(initial), parts.join("/"));
    if joined.is_empty() { ".".to_owned() } else { joined }
}

/// `posixpath.split(path)`: everything up to the last slash (trailing slashes stripped
/// unless it is all slashes), and the rest.
pub fn split(path: &str) -> (&str, &str) {
    let cut = path.rfind('/').map_or(0, |index| index + 1);
    let (head, tail) = path.split_at(cut);
    if !head.is_empty() && head.bytes().any(|byte| byte != b'/') {
        (head.trim_end_matches('/'), tail)
    } else {
        (head, tail)
    }
}

/// `posixpath.dirname(path)`.
pub fn dirname(path: &str) -> &str {
    split(path).0
}

/// `posixpath.basename(path)`.
pub fn basename(path: &str) -> &str {
    split(path).1
}

/// `posixpath.join(head, tail)`: an absolute `tail` replaces `head`.
pub fn join(head: &str, tail: &str) -> String {
    if tail.starts_with('/') {
        tail.to_owned()
    } else if head.is_empty() || head.ends_with('/') {
        format!("{head}{tail}")
    } else {
        format!("{head}/{tail}")
    }
}

#[cfg(test)]
mod tests {
    use super::{basename, dirname, join, normpath, split};
    use crate::golden;

    #[test]
    fn functions_match_posixpath() {
        let document = golden::load("text/posixpath.json");
        let cases = golden::cases(&document);
        assert!(cases.len() > 30, "too few cases");
        for case in cases {
            let path = golden::string(golden::get(case, "path"));
            let path = path.as_str();
            assert_eq!(
                normpath(path),
                golden::string(golden::get(case, "normpath")),
                "normpath {path:?}"
            );
            let expected = golden::list(golden::get(case, "split"));
            let (head, tail) = (golden::string(&expected[0]), golden::string(&expected[1]));
            assert_eq!(split(path), (head.as_str(), tail.as_str()), "split {path:?}");
            assert_eq!(
                dirname(path),
                golden::string(golden::get(case, "dirname")),
                "dirname {path:?}"
            );
            assert_eq!(
                basename(path),
                golden::string(golden::get(case, "basename")),
                "basename {path:?}"
            );
            for joined in golden::list(golden::get(case, "joins")) {
                let tail = golden::string(golden::get(joined, "tail"));
                assert_eq!(
                    join(path, &tail),
                    golden::string(golden::get(joined, "joined")),
                    "join {path:?} {tail:?}"
                );
            }
        }
    }
}
