//! The merge Jobs scenario (`tools/scenarios/specs/kube/jobs.toml`): the Rust port of
//! `tools/scenarios/operations/kube.py` runs the spec's steps through `KubeJobs` against the
//! Rust fake Kubernetes API and must render the bytes the Python baseline committed under
//! `goldens/scenarios/kube/`.

use std::time::Duration;

use embyx_clients::kube::{KubeJobs, KubeJobsError, KubeOptions, job_failure, job_outcome};
use embyx_clients::scenario::ScenarioFakes;
use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_core::scenario::{self, Parts, Recorder, Seeded, Spec};
use embyx_db::testdb::TestDatabase;

fn text<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("step argument {key:?} is a string"))
}

fn error_of(error: &KubeJobsError) -> Value {
    let mut dict = Dict::new();
    dict.insert("type", Value::from(error.class_name()));
    dict.insert("message", Value::Str(error.to_string()));
    Value::Dict(dict)
}

/// `_run`.
async fn run(kube: &KubeJobs, seeded: &Seeded, op: &str, args: &Value) -> Result<Value, KubeJobsError> {
    Ok(match op {
        "create_job" => {
            let job_args: Vec<String> = args
                .get("args")
                .and_then(Value::as_list)
                .expect("args")
                .iter()
                .map(|item| item.as_str().expect("a string").to_owned())
                .collect();
            let Some(Value::Int(task_id)) = args.get("task_id") else {
                panic!("task_id is an integer");
            };
            let task_id = i64::try_from(*task_id).expect("task_id fits");
            kube.create_job(text(args, "name"), &job_args, task_id).await?;
            Value::Null
        }
        "get_job" => match kube.get_job(text(args, "name")).await? {
            None => Value::Null,
            Some(job) => {
                let mut dict = Dict::new();
                dict.insert("outcome", Value::from(job_outcome(&job)));
                dict.insert("failure", Value::Str(job_failure(&job)));
                dict.insert("job", job);
                Value::Dict(dict)
            }
        },
        "delete_job" => {
            kube.delete_job(text(args, "name")).await?;
            Value::Null
        }
        "log_tail" => {
            let lines = match args.get("lines") {
                None => 20,
                Some(Value::Int(lines)) => u32::try_from(*lines).expect("lines fits"),
                Some(other) => panic!("lines is an integer, not {other:?}"),
            };
            Value::Str(kube.log_tail(text(args, "name"), lines).await?)
        }
        "own_image" => Value::Str(kube.own_image().await?),
        "write_template" => {
            // `json.dumps(args['template'])`.
            let template = args.get("template").expect("template");
            let written = pyjson::dumps(template, &DumpOptions::python_default()).expect("dumps");
            std::fs::write(seeded.root("mount").join("job.json"), written).expect("write the template");
            Value::Null
        }
        other => panic!("unknown step {other:?}"),
    })
}

#[tokio::test]
async fn kube_jobs_matches_the_python_transcript() {
    let spec = Spec::load("kube/jobs");
    assert_eq!(spec.operation(), "kube.steps");
    let seeded = Seeded::new(&spec);
    let database = TestDatabase::create().await;
    embyx_db::scenario::seed(database.pool(), &spec).await;
    let recorder = Recorder::new();
    let fakes = ScenarioFakes::start(&spec, &recorder).await;
    let args = spec.section("args");
    let kube = KubeJobs::new(&KubeOptions {
        api_url: fakes.kubernetes.url.clone(),
        namespace: text(&args, "namespace").to_owned(),
        pod_name: text(&args, "pod_name").to_owned(),
        template_path: seeded.root("mount").join("job.json"),
        token_path: seeded.root("mount").join("token"),
        ca_path: None,
        proxy: None,
        timeout: Duration::from_secs(30),
    })
    .expect("client");

    let mut reported = Vec::new();
    for step in args.get("steps").and_then(Value::as_list).unwrap_or_default() {
        let op = text(step, "op");
        let mut entry = Dict::new();
        entry.insert("op", Value::from(op));
        let step_args = step.get("args").cloned().unwrap_or_else(|| Value::Dict(Dict::new()));
        match run(&kube, &seeded, op, &step_args).await {
            Ok(value) => entry.insert("result", value),
            Err(error) => entry.insert("error", error_of(&error)),
        }
        reported.push(Value::Dict(entry));
    }
    let mut result = Dict::new();
    result.insert("steps", Value::List(reported));

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
