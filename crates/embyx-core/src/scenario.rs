//! The Rust side of the effect transcripts (docs/rewrite/SCENARIOS.md).
//!
//! A scenario (`tools/scenarios/specs/<area>/<name>.toml`) is a seed plus one operation. The
//! Python runner (`tools/scenarios/runner.py`) executes the baseline against it and commits
//! the transcript to `goldens/scenarios/<area>/<name>.json`. A Rust test loads the same spec,
//! seeds the same tree ([`Seeded`]), runs the ported operation against the Rust fakes (each
//! recording into one [`Recorder`]), dumps the rows (`embyx_db::scenario`) and the fakes'
//! state, and calls [`assert_transcript`], which renders the transcript exactly as
//! `tools/scenarios/transcript.py` does and compares bytes.
//!
//! Every normalization step below mirrors one function of `transcript.py`, in the same order:
//! root paths, call lanes and sequence numbers, UUIDs, volatile values.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sha2::{Digest as _, Sha256};

use crate::pyjson::{self, Dict, DumpOptions, Key, Value};

/// `transcript.FORMAT`.
pub const FORMAT: i128 = 1;

/// The repository root of this checkout.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A parsed scenario spec.
#[derive(Debug, Clone)]
pub struct Spec {
    /// `<area>/<name>`, the spec's path under `tools/scenarios/specs` without `.toml`.
    pub name: String,
    /// The whole TOML document as a JSON-shaped value.
    pub document: Value,
}

impl Spec {
    /// Load `tools/scenarios/specs/<name>.toml`.
    pub fn load(name: &str) -> Self {
        let path = repo_root().join("tools/scenarios/specs").join(format!("{name}.toml"));
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let table: toml::Table =
            toml::from_str(&text).unwrap_or_else(|error| panic!("{} is not TOML: {error}", path.display()));
        Self {
            name: name.to_owned(),
            document: from_toml(&toml::Value::Table(table)),
        }
    }

    /// A top-level key of the spec.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.document.get(key)
    }

    /// `spec['operation']`.
    pub fn operation(&self) -> &str {
        self.get("operation")
            .and_then(Value::as_str)
            .expect("a scenario names its operation")
    }

    /// `spec.get('clock', DEFAULT_CLOCK)` as written (RFC 3339).
    pub fn clock(&self) -> &str {
        self.get("clock")
            .and_then(Value::as_str)
            .unwrap_or("2026-01-01T00:00:00+00:00")
    }

    /// A section of the spec (`database`, `clouddrive`, `args`, ...), or an empty dict.
    pub fn section(&self, key: &str) -> Value {
        self.get(key).cloned().unwrap_or_else(|| Value::Dict(Dict::new()))
    }
}

fn from_toml(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(text) => Value::Str(text.clone()),
        toml::Value::Integer(int) => Value::Int(i128::from(*int)),
        toml::Value::Float(float) => Value::Float(*float),
        toml::Value::Boolean(flag) => Value::Bool(*flag),
        toml::Value::Array(items) => Value::List(items.iter().map(from_toml).collect()),
        toml::Value::Table(table) => {
            Value::Dict(table.iter().map(|(key, item)| (key.clone(), from_toml(item))).collect())
        }
        toml::Value::Datetime(datetime) => {
            panic!("scenario specs write times as strings, not TOML datetimes ({datetime})")
        }
    }
}

/// The seeded roots of one scenario, in a temporary directory removed on drop.
#[derive(Debug)]
pub struct Seeded {
    _base: tempfile::TempDir,
    /// Root name to its directory, in the spec's order.
    pub roots: Vec<(String, PathBuf)>,
}

impl Seeded {
    /// `runner.seed_tree`: create the roots and the tree entries in order.
    pub fn new(spec: &Spec) -> Self {
        let base = tempfile::Builder::new()
            .prefix("embyx-scenario-")
            .tempdir()
            .expect("temporary directory");
        let mut roots = Vec::new();
        if let Some(names) = spec
            .get("roots")
            .and_then(|roots| roots.get("names"))
            .and_then(Value::as_list)
        {
            for name in names {
                let name = name.as_str().expect("root names are strings").to_owned();
                let path = base.path().join(&name);
                std::fs::create_dir(&path).expect("create a root");
                roots.push((name, path));
            }
        }
        let seeded = Self { _base: base, roots };
        for entry in spec.get("tree").and_then(Value::as_list).unwrap_or_default() {
            seeded.create(entry);
        }
        seeded
    }

    /// The directory of a root.
    pub fn root(&self, name: &str) -> &Path {
        self.roots
            .iter()
            .find(|(root, _)| root == name)
            .map_or_else(|| panic!("no root {name:?}"), |(_, path)| path.as_path())
    }

    /// `<root>/<relative>` from a `root/relative` spec path.
    pub fn resolve(&self, spec_path: &str) -> PathBuf {
        let (root, relative) = spec_path.split_once('/').unwrap_or((spec_path, ""));
        self.root(root).join(relative)
    }

    fn create(&self, entry: &Value) {
        let path = self.resolve(
            entry
                .get("path")
                .and_then(Value::as_str)
                .expect("a tree entry has a path"),
        );
        let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("file");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parents");
        }
        match kind {
            "dir" => std::fs::create_dir_all(&path).expect("create a directory"),
            "file" => {
                let data = match (entry.get("text"), entry.get("size")) {
                    (Some(text), _) => text.as_str().expect("text is a string").as_bytes().to_vec(),
                    (None, Some(Value::Int(size))) => vec![b'x'; usize::try_from(*size).expect("a size")],
                    (None, _) => Vec::new(),
                };
                std::fs::write(&path, data).expect("write a file");
            }
            "symlink" => {
                let target = entry
                    .get("target")
                    .and_then(Value::as_str)
                    .expect("a symlink has a target");
                std::os::unix::fs::symlink(target, &path).expect("create a symlink");
            }
            "hardlink" => {
                let target = self.resolve(
                    entry
                        .get("target")
                        .and_then(Value::as_str)
                        .expect("a hard link has a target"),
                );
                std::fs::hard_link(target, &path).expect("create a hard link");
            }
            other => panic!("unknown tree entry kind {other:?}"),
        }
    }

    /// `transcript.snapshot_tree`: every entry under each root, sorted by path.
    pub fn snapshot(&self) -> Value {
        use std::os::unix::fs::MetadataExt as _;

        let mut tree = Dict::new();
        for (name, root) in &self.roots {
            let mut entries: Vec<(String, Value)> = Vec::new();
            let mut pending = vec![root.clone()];
            while let Some(directory) = pending.pop() {
                for item in std::fs::read_dir(&directory).expect("read a directory") {
                    let path = item.expect("a directory entry").path();
                    let info = std::fs::symlink_metadata(&path).expect("stat");
                    let relative = path
                        .strip_prefix(root)
                        .expect("under the root")
                        .to_str()
                        .expect("UTF-8 paths in scenarios")
                        .to_owned();
                    let mut entry = Dict::new();
                    entry.insert("path", Value::Str(relative.clone()));
                    if info.file_type().is_symlink() {
                        entry.insert("kind", Value::from("symlink"));
                        let target = std::fs::read_link(&path).expect("readlink");
                        entry.insert("target", Value::Str(target.to_str().expect("UTF-8 link").to_owned()));
                    } else if info.is_dir() {
                        entry.insert("kind", Value::from("dir"));
                        pending.push(path.clone());
                    } else {
                        let digest = hex::encode(Sha256::digest(std::fs::read(&path).expect("read a file")));
                        entry.insert("kind", Value::from("file"));
                        entry.insert("size", Value::Int(i128::from(info.len())));
                        entry.insert("nlink", Value::Int(i128::from(info.nlink())));
                        entry.insert("sha256", Value::Str(digest[..16].to_owned()));
                    }
                    entries.push((relative, Value::Dict(entry)));
                }
            }
            // Python sorts `str` by code point, which is UTF-8 byte order.
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            tree.insert(
                name.as_str(),
                Value::List(entries.into_iter().map(|(_, entry)| entry).collect()),
            );
        }
        Value::Dict(tree)
    }
}

/// Where every fake writes its calls, in the order they arrive.
#[derive(Debug, Clone, Default)]
pub struct Recorder {
    calls: Arc<Mutex<Vec<Value>>>,
}

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one call record (`{fake, method, request, response | error}`).
    pub fn record(&self, call: Value) {
        self.calls.lock().expect("recorder lock").push(call);
    }

    pub fn calls(&self) -> Vec<Value> {
        self.calls.lock().expect("recorder lock").clone()
    }
}

/// `{'result': value}`.
pub fn result(value: Value) -> Value {
    Value::Dict([("result".to_owned(), value)].into_iter().collect())
}

/// `{'error': {'type': ..., 'message': ...}}`, Python's exception class name and `str(exc)`.
pub fn error(class: &str, message: &str) -> Value {
    let inner: Dict = [
        ("type".to_owned(), Value::from(class)),
        ("message".to_owned(), Value::from(message)),
    ]
    .into_iter()
    .collect();
    Value::Dict([("error".to_owned(), Value::Dict(inner))].into_iter().collect())
}

/// Everything a transcript is made of, gathered by the test.
#[derive(Debug)]
pub struct Parts<'a> {
    pub spec: &'a Spec,
    pub seeded: &'a Seeded,
    pub outcome: Value,
    pub calls: Vec<Value>,
    pub rows: Value,
    pub fakes: Value,
}

/// `transcript.render`.
pub fn render(parts: Parts<'_>) -> String {
    let spec = parts.spec;
    let mut document = Dict::new();
    document.insert("format", Value::Int(FORMAT));
    document.insert("scenario", Value::Str(spec.name.clone()));
    document.insert("operation", Value::from(spec.operation()));
    document.insert("outcome", parts.outcome);
    document.insert("calls", Value::List(parts.calls));
    document.insert("tree", parts.seeded.snapshot());
    document.insert("rows", parts.rows);
    document.insert("fakes", parts.fakes);
    let mut document = Value::Dict(document);

    replace_roots(&mut document, &parts.seeded.roots);
    let ordering = spec.section("ordering");
    let mode = ordering
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("total")
        .to_owned();
    let lane_keys = ordering
        .get("lane_keys")
        .cloned()
        .unwrap_or_else(|| Value::Dict(Dict::new()));
    let calls = match document.as_dict().and_then(|dict| dict.get("calls")) {
        Some(Value::List(calls)) => calls.clone(),
        _ => Vec::new(),
    };
    set_key(
        &mut document,
        "calls",
        Value::List(order_calls(calls, &mode, &lane_keys)),
    );
    replace_uuids(&mut document);
    let volatile: Vec<String> = spec
        .get("volatile")
        .and_then(Value::as_list)
        .unwrap_or_default()
        .iter()
        .map(|selector| selector.as_str().expect("volatile selectors are strings").to_owned())
        .collect();
    replace_volatile(&mut document, &volatile);
    let mut text = pyjson::dumps(&document, &DumpOptions::pretty()).expect("a transcript dumps");
    text.push('\n');
    text
}

/// Render and compare with `goldens/scenarios/<name>.json`, showing the first differences.
pub fn assert_transcript(parts: Parts<'_>) {
    let name = parts.spec.name.clone();
    let rendered = render(parts);
    let path = repo_root().join("goldens/scenarios").join(format!("{name}.json"));
    let golden = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read {} (run tools/scenarios/runner.py): {error}",
            path.display()
        )
    });
    if rendered == golden {
        return;
    }
    let mut report = format!(
        "transcript of scenario {name} differs from Python's ({})\n",
        path.display()
    );
    let (want, have): (Vec<&str>, Vec<&str>) = (golden.lines().collect(), rendered.lines().collect());
    let mut shown = 0;
    for index in 0..want.len().max(have.len()) {
        let (left, right) = (want.get(index).copied(), have.get(index).copied());
        if left != right {
            let _ = writeln!(report, "line {}:\n  python: {left:?}\n  rust:   {right:?}", index + 1);
            shown += 1;
            if shown == 20 {
                break;
            }
        }
    }
    panic!("{report}");
}

// ---------- normalization, mirroring transcript.py ----------

fn set_key(document: &mut Value, key: &str, value: Value) {
    if let Value::Dict(dict) = document {
        dict.insert(key, value);
    }
}

/// `_walk_strings`: every string, keys included, through `change`.
fn walk_strings(value: &mut Value, change: &dyn Fn(&str) -> String) {
    match value {
        Value::Str(text) => *text = change(text),
        Value::List(items) => items.iter_mut().for_each(|item| walk_strings(item, change)),
        Value::Dict(dict) => {
            let mut rebuilt = Dict::new();
            for (key, item) in dict.iter() {
                let mut item = item.clone();
                walk_strings(&mut item, change);
                let key = match key {
                    Key::Str(text) => Key::Str(change(text)),
                    other => other.clone(),
                };
                rebuilt.insert(key, item);
            }
            *dict = rebuilt;
        }
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => {}
    }
}

/// `_ordered_strings`: keys sorted, each key before its value.
fn ordered_strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Str(text) => out.push(text.clone()),
        Value::List(items) => items.iter().for_each(|item| ordered_strings(item, out)),
        Value::Dict(dict) => {
            let mut entries: Vec<(&str, &Value)> = dict
                .iter()
                .map(|(key, item)| match key {
                    Key::Str(text) => (text.as_str(), item),
                    other => panic!("transcripts have string keys only, found {other:?}"),
                })
                .collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (key, item) in entries {
                out.push(key.to_owned());
                ordered_strings(item, out);
            }
        }
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => {}
    }
}

/// `_replace_roots`: each root's absolute path becomes `{name}`, longest path first.
fn replace_roots(document: &mut Value, roots: &[(String, PathBuf)]) {
    let mut ordered: Vec<(String, String)> = roots
        .iter()
        .map(|(name, path)| (name.clone(), path.to_str().expect("UTF-8 root").to_owned()))
        .collect();
    // Python's `sorted(..., reverse=True)` is stable too: equal lengths keep spec order.
    ordered.sort_by_key(|(_, path)| std::cmp::Reverse(path.chars().count()));
    walk_strings(document, &|text| {
        let mut text = text.to_owned();
        for (name, path) in &ordered {
            text = text.replace(path.as_str(), &format!("{{{name}}}"));
        }
        text
    });
}

static UUID: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}").expect("pattern")
});

/// `_replace_uuids`: numbered by first appearance in the canonical order of the sections.
fn replace_uuids(document: &mut Value) {
    let mut strings = Vec::new();
    for section in ["outcome", "calls", "tree", "rows", "fakes"] {
        if let Some(value) = document.get(section) {
            ordered_strings(value, &mut strings);
        }
    }
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for text in &strings {
        for found in UUID.find_iter(text) {
            let next = seen.len() + 1;
            seen.entry(found.as_str().to_lowercase()).or_insert(next);
        }
    }
    walk_strings(document, &|text| {
        UUID.replace_all(text, |captures: &regex::Captures<'_>| {
            format!("<uuid:{}>", seen[&captures[0].to_lowercase()])
        })
        .into_owned()
    });
}

/// `_get_path`: a dotted path through dicts and list indices.
fn get_path<'a>(mut value: &'a Value, dotted: &str) -> Option<&'a Value> {
    if dotted.is_empty() {
        return Some(value);
    }
    for part in dotted.split('.') {
        value = match value {
            Value::Dict(dict) => dict.get(part)?,
            Value::List(items) if !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()) => {
                items.get(part.parse::<usize>().ok()?)?
            }
            _ => return None,
        };
    }
    Some(value)
}

fn get_path_mut<'a>(mut value: &'a mut Value, dotted: &str) -> Option<&'a mut Value> {
    for part in dotted.split('.') {
        value = match value {
            Value::Dict(dict) => dict.get_mut(part)?,
            Value::List(items) => items.get_mut(part.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(value)
}

/// `_rank_key`: `json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))`.
fn rank_key(value: &Value) -> String {
    let options = DumpOptions {
        sort_keys: true,
        ..DumpOptions::compact()
    };
    pyjson::dumps(value, &options).expect("a value dumps")
}

/// `_replace_volatile`: each selected value becomes `<volatile:rank>`.
fn replace_volatile(document: &mut Value, volatile: &[String]) {
    for selector in volatile {
        let (section, rest) = selector.split_once('.').unwrap_or((selector, ""));
        // (index into the section list, path inside that element)
        let places: Vec<(usize, String)> = match section {
            "rows" => {
                let (table, column) = rest.split_once('.').expect("rows.<table>.<column>");
                let rows = get_path(document, &format!("rows.{table}"))
                    .and_then(Value::as_list)
                    .unwrap_or_default();
                rows.iter()
                    .enumerate()
                    .filter(|(_, row)| row.as_dict().is_some_and(|dict| dict.contains_key(column)))
                    .map(|(index, _)| (index, column.to_owned()))
                    .collect()
            }
            "calls" => {
                let mut split = rest.splitn(3, '.');
                let (fake, method, path) = (
                    split.next().unwrap_or_default(),
                    split.next().unwrap_or_default(),
                    split.next().unwrap_or_default(),
                );
                let calls = document.get("calls").and_then(Value::as_list).unwrap_or_default();
                calls
                    .iter()
                    .enumerate()
                    .filter(|(_, call)| {
                        call.get("fake").and_then(Value::as_str) == Some(fake)
                            && call.get("method").and_then(Value::as_str) == Some(method)
                            && get_path(call, path).is_some_and(|value| !matches!(value, Value::Null))
                    })
                    .map(|(index, _)| (index, path.to_owned()))
                    .collect()
            }
            _ => panic!("volatile selector {selector:?} must start with rows. or calls."),
        };
        let list_path = match section {
            "rows" => format!("rows.{}", rest.split_once('.').map_or(rest, |(table, _)| table)),
            _ => "calls".to_owned(),
        };
        let holder_path = |index: usize, path: &str| format!("{list_path}.{index}.{path}");
        let values: BTreeSet<String> = places
            .iter()
            .map(|(index, path)| rank_key(get_path(document, &holder_path(*index, path)).expect("selected")))
            .collect();
        let ranks: BTreeMap<String, usize> = values
            .into_iter()
            .enumerate()
            .map(|(rank, key)| (key, rank + 1))
            .collect();
        for (index, path) in &places {
            let full = holder_path(*index, path);
            let key = rank_key(get_path(document, &full).expect("selected"));
            *get_path_mut(document, &full).expect("selected") = Value::Str(format!("<volatile:{}>", ranks[&key]));
        }
    }
}

/// `_order_calls`: lanes and per-lane sequence numbers.
fn order_calls(calls: Vec<Value>, mode: &str, lane_keys: &Value) -> Vec<Value> {
    assert!(
        mode == "total" || mode == "lanes",
        "ordering mode {mode:?} is neither total nor lanes"
    );
    let mut laned: Vec<(String, Value)> = calls
        .into_iter()
        .map(|call| {
            let mut lane = "main".to_owned();
            if mode == "lanes" {
                let fake = call.get("fake").and_then(Value::as_str).unwrap_or_default();
                if let Some(key) = lane_keys.get(fake).and_then(Value::as_str) {
                    match get_path(&call, key) {
                        Some(Value::Str(text)) => lane.clone_from(text),
                        // Python's `_get_path` treats an explicit null like a missing key.
                        Some(Value::Null) | None => {}
                        Some(other) => lane = rank_key(other),
                    }
                }
            }
            assert!(
                !UUID.is_match(&lane),
                "lane {lane:?} contains a UUID; choose a lane key without one"
            );
            (lane, call)
        })
        .collect();
    if mode == "lanes" {
        // `list.sort` is stable, and so is `sort_by`: the order inside a lane is kept.
        laned.sort_by(|a, b| a.0.cmp(&b.0));
    }
    let mut counters: BTreeMap<String, i128> = BTreeMap::new();
    laned
        .into_iter()
        .map(|(lane, call)| {
            let counter = counters.entry(lane.clone()).or_insert(0);
            *counter += 1;
            let mut dict = match call {
                Value::Dict(dict) => dict,
                other => panic!("a call record is a dict, not {other:?}"),
            };
            dict.insert("lane", Value::Str(lane));
            dict.insert("seq", Value::Int(*counter));
            Value::Dict(dict)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Value, order_calls, rank_key, replace_uuids};
    use crate::pyjson;

    fn json(text: &str) -> Value {
        pyjson::loads(text).expect("JSON")
    }

    #[test]
    fn lanes_group_calls_and_keep_their_order_inside_a_lane() {
        let calls = json(
            r#"[{"fake": "cd", "method": "M", "request": {"p": "b"}},
                {"fake": "cd", "method": "M", "request": {"p": "a"}},
                {"fake": "cd", "method": "N", "request": {"p": "b"}},
                {"fake": "kube", "method": "GET", "request": {}}]"#,
        );
        let ordered = order_calls(
            calls.as_list().unwrap().to_vec(),
            "lanes",
            &json(r#"{"cd": "request.p"}"#),
        );
        let summary: Vec<(String, i128, String)> = ordered
            .iter()
            .map(|call| {
                (
                    call.get("lane").and_then(Value::as_str).unwrap().to_owned(),
                    match call.get("seq") {
                        Some(Value::Int(seq)) => *seq,
                        _ => 0,
                    },
                    call.get("method").and_then(Value::as_str).unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                ("a".to_owned(), 1, "M".to_owned()),
                ("b".to_owned(), 1, "M".to_owned()),
                ("b".to_owned(), 2, "N".to_owned()),
                ("main".to_owned(), 1, "GET".to_owned()),
            ]
        );
    }

    #[test]
    fn uuids_are_numbered_by_first_appearance_with_sorted_keys() {
        let mut document = json(
            r#"{"outcome": {"z": "6F9619FF-8B86-D011-B42D-00CF4FC964FF", "a": "x 123e4567-e89b-12d3-a456-426614174000"},
                "calls": [], "tree": {}, "rows": {}, "fakes": {"k": "6f9619ff-8b86-d011-b42d-00cf4fc964ff"}}"#,
        );
        replace_uuids(&mut document);
        assert_eq!(
            document.get("outcome").and_then(|o| o.get("a")).and_then(Value::as_str),
            Some("x <uuid:1>")
        );
        assert_eq!(
            document.get("outcome").and_then(|o| o.get("z")).and_then(Value::as_str),
            Some("<uuid:2>")
        );
        assert_eq!(
            document.get("fakes").and_then(|o| o.get("k")).and_then(Value::as_str),
            Some("<uuid:2>")
        );
    }

    #[test]
    fn rank_keys_are_compact_sorted_json() {
        assert_eq!(
            rank_key(&json(r#"{"b": 1, "a": [1.0, "é"]}"#)),
            r#"{"a":[1.0,"é"],"b":1}"#
        );
    }
}
