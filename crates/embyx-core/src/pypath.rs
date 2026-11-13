//! `pathlib.PurePosixPath` arithmetic: joining, `name`, `suffix`, `stem`.
//!
//! Python origin: `pathlib` in `CPython` 3.13. Paths stored as `str(Path)` (library paths,
//! merge parts, the move journal) have to come out the way pathlib writes them: no trailing
//! slash, `.` parts dropped, `//` collapsed.
//!
//! Copied from fav (`fav-core/src/pypath.rs`, with `PyPath` from `fav-core/src/validate.rs`);
//! see docs/CONVENTIONS.md, "Code copied from fav".

use std::fmt;

/// `str(PurePosixPath(text))`: `.` parts dropped, a trailing slash dropped, `..` kept, a
/// leading `//` (exactly two) kept, and the empty path written as `.`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PyPath(String);

impl PyPath {
    pub fn new(text: &str) -> Self {
        let root = if text.starts_with("//") && !text.starts_with("///") {
            "//"
        } else if text.starts_with('/') {
            "/"
        } else {
            ""
        };
        let parts: Vec<&str> = text
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .collect();
        let joined = format!("{root}{}", parts.join("/"));
        Self(if joined.is_empty() { ".".to_owned() } else { joined })
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.0)
    }

    /// `path / part`: pathlib drops a lone `.`, so `Path('.') / 'a'` is `a`, not `./a`.
    pub fn join(&self, part: impl AsRef<std::path::Path>) -> std::path::PathBuf {
        if self.0 == "." {
            part.as_ref().to_path_buf()
        } else {
            self.as_path().join(part)
        }
    }
}

impl fmt::Display for PyPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `str(PurePosixPath(base) / part)`: `part` replaces `base` when it is absolute.
pub fn join(base: &str, part: &str) -> String {
    if part.starts_with('/') {
        return PyPath::new(part).as_str().to_owned();
    }
    PyPath::new(&format!("{base}/{part}")).as_str().to_owned()
}

/// `PurePosixPath(path).name`: the last part, empty for `/`, `.` or the empty path.
pub fn name(path: &str) -> &str {
    path.rsplit('/')
        .find(|part| !part.is_empty() && *part != ".")
        .unwrap_or("")
}

/// `PurePosixPath(path).suffix`: from the last dot of the name, unless the dot starts or
/// ends it.
pub fn suffix(path: &str) -> &str {
    let name = name(path);
    match name.rfind('.') {
        Some(dot) if dot > 0 && dot < name.len() - 1 => &name[dot..],
        _ => "",
    }
}

/// `PurePosixPath(path).stem`: the name without its suffix.
pub fn stem(path: &str) -> &str {
    let name = name(path);
    let suffix = suffix(path);
    &name[..name.len() - suffix.len()]
}

#[cfg(test)]
mod tests {
    use super::{join, name, stem, suffix};
    use crate::golden;

    #[test]
    fn name_suffix_stem_and_join_match_pathlib() {
        let document = golden::load("pyweb/paths.json");
        let case = &golden::cases(&document)[0];
        for part in golden::list(golden::get(case, "parts")) {
            let path = golden::string(golden::get(part, "path"));
            assert_eq!(name(&path), golden::string(golden::get(part, "name")), "{path:?}");
            assert_eq!(suffix(&path), golden::string(golden::get(part, "suffix")), "{path:?}");
            assert_eq!(stem(&path), golden::string(golden::get(part, "stem")), "{path:?}");
        }
        for entry in golden::list(golden::get(case, "joins")) {
            let base = golden::string(golden::get(entry, "base"));
            let part = golden::string(golden::get(entry, "part"));
            assert_eq!(
                join(&base, &part),
                golden::string(golden::get(entry, "joined")),
                "{base:?} {part:?}"
            );
        }
    }
}
