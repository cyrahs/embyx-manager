//! `home_worker_cache.json`: sidecars written and read back, and which kept chunks a resumed
//! part carries on from (the cache layout of contract 5).

use std::fs;
use std::path::Path;

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::{cases, float, object};
use crate::chunks::{self, LocalChunk};

fn chunk(fields: &Value, path: &Path) -> LocalChunk {
    LocalChunk {
        part_index: golden::int(golden::get(fields, "part_index")),
        index: golden::int(golden::get(fields, "index")),
        start: float(golden::get(fields, "start")),
        duration: float(golden::get(fields, "duration")),
        frames: golden::int(golden::get(fields, "frames")),
        bytes: golden::int(golden::get(fields, "bytes")),
        sha256: golden::string(golden::get(fields, "sha256")),
        path: path.to_owned(),
    }
}

fn listing(directory: &Path) -> Value {
    let mut names: Vec<_> = fs::read_dir(directory)
        .expect("listing")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    Value::List(
        names
            .into_iter()
            .map(|name| {
                let text = String::from_utf8_lossy(&fs::read(directory.join(&name)).expect("read")).into_owned();
                object([("name", Value::Str(name)), ("text", Value::Str(text))])
            })
            .collect(),
    )
}

/// The chunk's fields as the golden shows them: `start` and `duration` compared as numbers,
/// since Python keeps an int a sidecar gave it.
fn view(chunk: &LocalChunk, root: &Path) -> Value {
    let path = chunk.path.to_string_lossy().replace(&*root.to_string_lossy(), "<root>");
    object([
        ("part_index", Value::Int(chunk.part_index)),
        ("index", Value::Int(chunk.index)),
        ("start", Value::Float(chunk.start)),
        ("duration", Value::Float(chunk.duration)),
        ("frames", Value::Int(chunk.frames)),
        ("bytes", Value::Int(chunk.bytes)),
        ("sha256", Value::Str(chunk.sha256.clone())),
        ("path", Value::Str(super::slashes(&path))),
    ])
}

fn numbers_as_floats(value: &Value) -> Value {
    let mut dict = super::dict(value);
    for key in ["start", "duration"] {
        let number = float(golden::get(value, key));
        dict.insert(key, Value::Float(number));
    }
    Value::Dict(dict)
}

#[test]
fn sidecars_and_kept_chunks_match_python() {
    let root = tempfile::tempdir().expect("temporary directory");
    let root = root.path();
    let (mut saves, mut loads, mut kept) = (0, 0, 0);
    for case in cases("cache") {
        match golden::string(golden::get(&case, "kind")).as_str() {
            "save" => {
                let folder = root.join(format!("save{saves}"));
                fs::create_dir(&folder).expect("folder");
                let fields = golden::get(&case, "chunk");
                let name = format!("chunk-{:05}.mkv", golden::int(golden::get(fields, "index")));
                let chunk = chunk(fields, &folder.join(name));
                chunk.save().expect("save");
                assert_eq!(Value::Float(chunk.end()), *golden::get(&case, "end"), "case {case:?}");
                assert_eq!(listing(&folder), *golden::get(&case, "files"), "case {case:?}");
                saves += 1;
            }
            "load" => {
                let folder = root.join(format!("load{loads}"));
                fs::create_dir(&folder).expect("folder");
                let sidecar = folder.join("chunk-00000.json");
                let text = golden::get(&case, "text").as_str().unwrap_or_default().to_owned();
                fs::write(&sidecar, text).expect("sidecar");
                if let Value::Int(size) = golden::get(&case, "size") {
                    fs::write(
                        folder.join("chunk-00000.mkv"),
                        vec![b'x'; usize::try_from(*size).expect("size")],
                    )
                    .expect("chunk");
                }
                let got = LocalChunk::load(&sidecar).map_or(Value::Null, |chunk| view(&chunk, root));
                let want = match golden::get(&case, "value") {
                    Value::Null => Value::Null,
                    value => numbers_as_floats(value),
                };
                assert_eq!(got, want, "case {case:?}");
                loads += 1;
            }
            "kept" if case.get("files").is_none() => {
                let found = chunks::kept_chunks(&root.join("nowhere"), 0, 0.0);
                assert!(found.is_empty());
                assert_eq!(golden::get(&case, "kept"), &Value::List(Vec::new()));
            }
            "kept" => {
                let folder = root.join(format!("kept{kept}"));
                fs::create_dir(&folder).expect("folder");
                for entry in golden::list(golden::get(&case, "files")) {
                    let name = golden::string(golden::get(entry, "name"));
                    let path = folder.join(&name);
                    if entry.get("dir").is_some() {
                        fs::create_dir(&path).expect("dir");
                    } else if let Some(fields) = entry.get("chunk") {
                        let chunk = chunk(fields, &path);
                        fs::write(&path, vec![b'x'; usize::try_from(chunk.bytes).expect("size")]).expect("chunk");
                        chunk.save().expect("save");
                    } else {
                        let text = entry.get("text").and_then(Value::as_str).unwrap_or_default();
                        fs::write(&path, text).expect("file");
                    }
                }
                let found = chunks::kept_chunks(
                    &folder,
                    golden::int(golden::get(&case, "next_index")),
                    float(golden::get(&case, "resume_at")),
                );
                let indices = Value::List(found.iter().map(|chunk| Value::Int(chunk.index)).collect());
                assert_eq!(&indices, golden::get(&case, "kept"), "case {case:?}");
                let mut left: Vec<_> = fs::read_dir(&folder)
                    .expect("listing")
                    .map(|entry| Value::Str(entry.expect("entry").file_name().to_string_lossy().into_owned()))
                    .collect();
                left.sort_by_key(|name| name.as_str().unwrap_or_default().to_owned());
                assert_eq!(&Value::List(left), golden::get(&case, "left"), "case {case:?}");
                kept += 1;
            }
            other => panic!("unknown kind {other}"),
        }
    }
    assert_eq!((saves, loads, kept), (3, 9, 7));
}
