//! The Rust side of the harness scenario (docs/rewrite/SCENARIOS.md): `harness/trivial`
//! proves that a Rust run renders the same transcript bytes as the Python runner
//! (`goldens/scenarios/harness/trivial.json`). It ports `tools/scenarios/operations/harness.py`
//! only; the WPs that port an operation add its scenarios beside this file.

use chrono::DateTime;
use embyx_clients::clouddrive::fake::to_json;
use embyx_clients::clouddrive::proto::cloud_drive_file_srv_client::CloudDriveFileSrvClient;
use embyx_clients::clouddrive::proto::{MoveFileRequest, move_file_request::ConflictPolicy};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{Dict, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;
use tonic::metadata::MetadataValue;

#[tokio::test]
async fn harness_trivial_matches_the_python_transcript() {
    let spec = Spec::load("harness/trivial");
    assert_eq!(spec.operation(), "harness.trivial");
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let now = DateTime::parse_from_rfc3339(spec.clock()).expect("clock").to_utc();

    // operations/harness.py `trivial`.
    let source = seeded.root("upload").join("in/a.txt");
    let target = seeded.root("upload").join("out/a.txt");
    std::fs::create_dir(target.parent().expect("parent")).expect("mkdir");
    std::fs::rename(&source, &target).expect("rename");
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", fakes.clouddrive.address))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = CloudDriveFileSrvClient::new(channel);
    let mut request = tonic::Request::new(MoveFileRequest {
        the_file_paths: vec!["/115/in/a.txt".to_owned()],
        dest_path: "/115/out".to_owned(),
        conflict_policy: Some(ConflictPolicy::Skip.into()),
        ..Default::default()
    });
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer scenario-token").expect("ascii"),
    );
    let moved = client.move_file(request).await.expect("MoveFile").into_inner();
    sqlx::query("UPDATE items SET state = 'moved', moved_at = $1 WHERE name = $2")
        .bind(now)
        .bind("a.txt")
        .execute(database.pool())
        .await
        .expect("update");
    let mut result = Dict::new();
    result.insert("move", to_json(&moved, "clouddrive.FileOperationResult"));
    result.insert("target", Value::Str(target.display().to_string()));

    let rows = embyx_db::scenario::dump_rows(database.pool(), &spec).await;
    let fakes_dump = fakes.dump();
    drop(fakes);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome: scenario::result(Value::Dict(result)),
        calls: recorder.calls(),
        rows,
        fakes: fakes_dump,
    });
    database.drop_database().await;
}
