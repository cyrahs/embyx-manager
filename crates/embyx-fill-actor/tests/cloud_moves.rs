//! Ported from `tests/test_cloud_moves.py`.

use std::path::{Path, PathBuf};

use embyx_core::pyjson::{self, Dict, Value};
use embyx_fill_actor::cloud_moves::{CheckError, CloudFileMetadata, CloudMovePaths, CloudMoveResponse};

fn cloud_paths() -> CloudMovePaths {
    let roots: Vec<String> = ["source-a", "source-b", "source-c", "source-d"]
        .iter()
        .map(|root| format!("/cloud/library/{root}"))
        .collect();
    CloudMovePaths::from_values("/mounted-cloud", &roots, "/cloud/library/destination").unwrap()
}

fn write_mapping(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

fn mapping(json: &str) -> Dict {
    match pyjson::loads(json).unwrap() {
        Value::Dict(dict) => dict,
        other => panic!("not an object: {other:?}"),
    }
}

// py: tests/test_cloud_moves.py::test_parses_one_strm_path_and_strips_only_the_configured_mount_prefix
#[test]
fn parses_one_strm_path_and_strips_only_the_configured_mount_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = cloud_paths();
    let file = write_mapping(
        tmp.path(),
        "ABC-001.strm",
        b"/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4\n",
    );

    let parsed = paths.parse_mapping(&file, 1).unwrap();

    assert_eq!(
        parsed.mounted_path,
        "/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4"
    );
    assert_eq!(parsed.api_path, "/cloud/library/source-b/ABC/ABC-001.mp4");
    assert_eq!(parsed.mapping_sha256.len(), 64);
    assert_eq!(
        paths.destination_directory("ABC").unwrap(),
        "/cloud/library/destination/ABC"
    );
}

// py: tests/test_cloud_moves.py::test_rejects_unsafe_or_cross_root_strm_targets cases=8
#[test]
fn rejects_unsafe_or_cross_root_strm_targets() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = cloud_paths();
    for value in [
        "",
        "relative/video.mp4",
        "https://example.invalid/video.mp4",
        "/mounted-cloud/cloud/library/source-b/../escape/ABC-001.mp4",
        "/mounted-cloud/cloud/library/source-a/ABC-001.mp4",
        "/mounted-cloud/cloud/library/source-b/ABC-001.txt",
        "/mounted-cloud/cloud/library/source-b/ABC-001.mp4\n/another.mp4",
        " /mounted-cloud/cloud/library/source-b/ABC-001.mp4",
    ] {
        let file = write_mapping(tmp.path(), "ABC-001.strm", value.as_bytes());
        // `pytest.raises((InvalidStrmTargetError, ValueError))`: every refusal here is one.
        let error = paths.parse_mapping(&file, 1).unwrap_err();
        assert!(error.is_value_error(), "{value:?}: {error:?}");
    }
}

// py: tests/test_cloud_moves.py::test_rejects_oversized_or_non_utf8_mapping
#[test]
fn rejects_oversized_or_non_utf8_mapping() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = cloud_paths();
    let mut oversized = b"/".to_vec();
    oversized.extend([b'a'; 4096]);
    let oversized = write_mapping(tmp.path(), "oversized.strm", &oversized);
    let invalid = write_mapping(tmp.path(), "invalid.strm", b"\xff");

    assert!(matches!(
        paths.parse_mapping(&oversized, 1),
        Err(CheckError::InvalidStrmTarget(_))
    ));
    assert!(matches!(
        paths.parse_mapping(&invalid, 1),
        Err(CheckError::InvalidStrmTarget(_))
    ));
}

// py: tests/test_cloud_moves.py::test_normalizes_runtime_metadata_without_accepting_untrusted_shapes
#[test]
fn normalizes_runtime_metadata_without_accepting_untrusted_shapes() {
    let metadata = CloudFileMetadata::from_mapping(&mapping(
        r#"{"path": "/cloud/library/source-b/ABC/ABC-001.mp4", "id": "file-id", "name": "ABC-001.mp4",
            "size": 12, "write_time": 34, "hashes": {"sha1": "abcd"}}"#,
    ))
    .unwrap();

    assert!(metadata.matches_identity(&metadata));
    assert_eq!(metadata.hashes, vec![("sha1".to_owned(), "abcd".to_owned())]);
    let error = CloudFileMetadata::from_mapping(&mapping(
        r#"{"path": "/cloud/library/source-b/ABC/not-the-name.mp4", "id": "file-id", "name": "ABC-001.mp4",
            "size": 12, "write_time": 34}"#,
    ))
    .unwrap_err();
    assert!(error.is_value_error());
    assert!(error.to_string().contains("identity"), "{error}");
}

// py: tests/test_cloud_moves.py::test_normalizes_move_response_without_exposing_runtime_types
#[test]
fn normalizes_move_response_without_exposing_runtime_types() {
    let response = CloudMoveResponse::from_mapping(&mapping(
        r#"{"success": true, "error_message": "", "result_paths": ["/cloud/library/destination/ABC/ABC-001.mp4"]}"#,
    ))
    .unwrap();

    assert!(response.success);
    assert_eq!(response.error_message, None);
    assert_eq!(
        response.result_paths,
        vec!["/cloud/library/destination/ABC/ABC-001.mp4".to_owned()]
    );
}
