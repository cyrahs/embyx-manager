//! Tests of the merge Jobs client: the `merge.kube` tests of `tests/test_merge_service.py`,
//! ported, plus the goldens of `tools/goldens/areas/kube.py`, error texts and transport
//! failures.
//!
//! Python's client test answers through httpx's `MockTransport`; here the client talks HTTPS
//! to the fake web ([`FakeWeb`], as its proxy, the way Python's `transport=` stands in), and
//! trusts the fake web's authority through a CA file, as the pod trusts the cluster's.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use embyx_core::golden;
use embyx_core::pyjson::{self, DumpOptions, Value};
use serde_json::json;

use super::{
    KubeJobs, KubeJobsError, KubeOptions, MAX_JOB_NAME, REQUEST_TIMEOUT, TASK_LABEL, build_job, job_failure, job_name,
    job_outcome,
};
use crate::net::fakeweb::{FakeWeb, Reply, Seen, test_roots};

fn value(json: &serde_json::Value) -> Value {
    Value::from_serde(json)
}

/// `value[a][b][0]...`: digits index a list.
fn at<'a>(value: &'a Value, path: &[&str]) -> &'a Value {
    path.iter().fold(value, |value, key| match key.parse::<usize>() {
        Ok(index) => &value.as_list().expect("a list")[index],
        Err(_) => value.get(key).unwrap_or_else(|| panic!("no {key}")),
    })
}

fn dumps(value: &Value) -> String {
    pyjson::dumps(value, &DumpOptions::plain()).expect("dumps")
}

/// A service-account-like directory: template, token and the fake web's CA as PEM.
struct Mounted {
    dir: tempfile::TempDir,
}

impl Mounted {
    fn new(template: &serde_json::Value) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("job.json"), template.to_string()).expect("template");
        std::fs::write(dir.path().join("token"), "secret-token\n").expect("token");
        let der = &test_roots()[0];
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let lines: Vec<&str> = body
            .as_bytes()
            .chunks(64)
            .map(|chunk| std::str::from_utf8(chunk).expect("base64"))
            .collect();
        let pem = format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            lines.join("\n")
        );
        std::fs::write(dir.path().join("ca.crt"), pem).expect("ca");
        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn options(&self, web: &FakeWeb) -> KubeOptions {
        KubeOptions {
            api_url: "https://kube.test".to_owned(),
            namespace: "media".to_owned(),
            pod_name: "app-0".to_owned(),
            template_path: self.path("job.json"),
            token_path: self.path("token"),
            ca_path: Some(self.path("ca.crt")),
            proxy: Some(web.proxy.clone()),
            timeout: REQUEST_TIMEOUT,
        }
    }
}

fn minimal_template() -> serde_json::Value {
    json!({"metadata": {}, "spec": {"template": {"spec": {"containers": [{"name": "merge"}]}}}})
}

fn pod(image: &str, image_id: &str) -> serde_json::Value {
    json!({
        "spec": {"containers": [{"image": image}]},
        "status": {"containerStatuses": [{"imageID": image_id}]},
    })
}

// py: tests/test_merge_service.py::test_job_names_are_dns_labels
#[test]
fn job_names_are_dns_labels() {
    assert_eq!(job_name(7, "SQTEVR-009"), "embyx-merge-7-sqtevr-009");
    assert_eq!(
        job_name(12345, &"A".repeat(80)),
        format!("embyx-merge-12345-{}", "a".repeat(34))
    );
    assert!(job_name(1, &"X_Y.Z".repeat(20)).len() <= 52);
    assert_eq!(MAX_JOB_NAME, 52);
}

// py: tests/test_merge_service.py::test_build_job_fills_in_name_image_args_and_label
#[test]
fn build_job_fills_in_name_image_args_and_label() {
    let template = value(&json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"labels": {"app": "embyx-merge"}},
        "spec": {
            "template": {"spec": {"containers": [{"name": "merge", "command": ["embyx-manager", "merge-worker"]}]}}
        },
    }));

    let job = build_job(
        &template,
        "embyx-merge-1-abc",
        "ghcr.io/x@sha256:1",
        &["--output".to_owned(), "o".to_owned()],
        1,
    )
    .expect("job");

    let metadata = job.get("metadata").and_then(Value::as_dict).expect("metadata");
    assert_eq!(metadata.get("name"), Some(&Value::from("embyx-merge-1-abc")));
    assert_eq!(
        dumps(metadata.get("labels").expect("labels")),
        r#"{"app": "embyx-merge", "embyx-manager/merge-task": "1"}"#
    );
    assert_eq!(metadata.len(), 2);
    let container = at(&job, &["spec", "template", "spec", "containers", "0"]);
    assert_eq!(container.get("image"), Some(&Value::from("ghcr.io/x@sha256:1")));
    assert_eq!(dumps(container.get("args").expect("args")), r#"["--output", "o"]"#);
    assert!(
        template
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .is_none()
    );
}

// py: tests/test_merge_service.py::test_job_outcome_reads_conditions_and_counters
#[test]
fn job_outcome_reads_conditions_and_counters() {
    assert_eq!(
        job_outcome(&value(
            &json!({"status": {"conditions": [{"type": "Complete", "status": "True"}]}})
        )),
        "succeeded"
    );
    assert_eq!(
        job_outcome(&value(
            &json!({"status": {"conditions": [{"type": "Failed", "status": "True"}]}})
        )),
        "failed"
    );
    assert_eq!(job_outcome(&value(&json!({"status": {"failed": 1}}))), "failed");
    assert_eq!(job_outcome(&value(&json!({"status": {"active": 1}}))), "running");
}

// py: tests/test_merge_service.py::test_kube_client_runs_the_job_on_the_pods_own_image
#[tokio::test]
async fn kube_client_runs_the_job_on_the_pods_own_image() {
    let mounted = Mounted::new(&minimal_template());
    let web = FakeWeb::start(|request: &Seen| {
        if request.path() == "/api/v1/namespaces/media/pods/app-0" {
            return Reply::json(
                200,
                &pod(
                    "ghcr.io/cyrahs/embyx-manager:latest",
                    "ghcr.io/cyrahs/embyx-manager@sha256:abc",
                ),
            );
        }
        if request.method == "POST" {
            return Reply::json(409, &json!({"reason": "AlreadyExists"}));
        }
        Reply::json(404, &json!({}))
    })
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");

    kube.create_job("embyx-merge-1-x", &["a".to_owned()], 1)
        .await
        .expect("409 counts as started");
    assert_eq!(kube.get_job("embyx-merge-1-x").await.expect("get"), None);

    let seen = web.seen();
    assert_eq!(seen[1].method, "POST");
    assert_eq!(seen[1].url, "https://kube.test/apis/batch/v1/namespaces/media/jobs");
    let created = pyjson::loads(std::str::from_utf8(&seen[1].body).expect("utf-8")).expect("JSON body");
    assert_eq!(
        at(&created, &["spec", "template", "spec", "containers", "0"]).get("image"),
        Some(&Value::from("ghcr.io/cyrahs/embyx-manager@sha256:abc"))
    );
    assert_eq!(seen[1].header("Authorization"), Some("Bearer secret-token"));
}

// ---------- goldens ----------

#[test]
fn job_names_match_python() {
    let document = golden::load("kube/job_names.json");
    for case in golden::cases(&document) {
        let task_id = i64::try_from(golden::int(golden::get(case, "task_id"))).expect("i64");
        let avid = golden::string(golden::get(case, "avid"));
        assert_eq!(
            job_name(task_id, &avid),
            golden::string(golden::get(case, "name")),
            "{task_id} {avid:?}"
        );
    }
}

#[test]
fn built_jobs_and_template_errors_match_python() {
    let document = golden::load("kube/build_job.json");
    let cases = golden::cases(&document);
    assert!(cases.len() >= 20);
    let args = ["--output".to_owned(), "o".to_owned(), "中文".to_owned()];
    for case in cases {
        let template = golden::get(case, "template");
        let result = build_job(
            template,
            "embyx-merge-1-abc",
            "ghcr.io/cyrahs/embyx-manager@sha256:1",
            &args,
            1,
        );
        match (case.get("value"), result) {
            (Some(expected), Ok(job)) => assert_eq!(dumps(&job), dumps(expected), "{}", dumps(template)),
            (None, Err(error)) => {
                let expected = golden::get(case, "error");
                assert_eq!(
                    (error.class_name(), error.to_string()),
                    (
                        golden::string(golden::get(expected, "type")).as_str(),
                        golden::string(golden::get(expected, "message"))
                    ),
                    "{}",
                    dumps(template)
                );
            }
            (expected, result) => panic!("{}: expected {expected:?}, got {result:?}", dumps(template)),
        }
    }
}

#[test]
fn job_outcomes_and_failures_match_python() {
    let document = golden::load("kube/job_status.json");
    for case in golden::cases(&document) {
        let job = golden::get(case, "job");
        assert_eq!(
            job_outcome(job),
            golden::string(golden::get(case, "outcome")),
            "{}",
            dumps(job)
        );
        assert_eq!(
            job_failure(job),
            golden::string(golden::get(case, "failure")),
            "{}",
            dumps(job)
        );
    }
}

// ---------- requests and errors ----------

/// A fake web that answers each request with the next scripted reply.
async fn scripted(replies: Vec<Reply>) -> FakeWeb {
    let replies = Arc::new(Mutex::new(replies.into_iter()));
    FakeWeb::start(move |_: &Seen| replies.lock().expect("replies").next().expect("a scripted reply")).await
}

#[tokio::test]
async fn requests_carry_the_token_the_json_body_and_httpx_query_encoding() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![
        Reply::json(200, &json!({"status": {}})),
        Reply::json(200, &json!({"kind": "Status"})),
        Reply::json(
            200,
            &json!({"items": [{"metadata": {"name": "old"}}, {"metadata": {"name": "embyx-merge-1-x-abcde"}}]}),
        ),
        Reply::bytes(200, "line one\nline two\n\n"),
    ])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");

    let job = kube.get_job("embyx-merge-1-x").await.expect("get").expect("a job");
    assert_eq!(dumps(&job), r#"{"status": {}}"#);
    kube.delete_job("embyx-merge-1-x").await.expect("delete");
    assert_eq!(
        kube.log_tail("embyx-merge-1-x", 20).await.expect("tail"),
        "line one\nline two"
    );

    let seen = web.seen();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(
        seen[0].url,
        "https://kube.test/apis/batch/v1/namespaces/media/jobs/embyx-merge-1-x"
    );
    assert_eq!(seen[1].method, "DELETE");
    assert_eq!(seen[1].body, br#"{"propagationPolicy":"Background"}"#);
    assert_eq!(seen[1].header("Content-Type"), Some("application/json"));
    assert_eq!(
        seen[2].url,
        "https://kube.test/api/v1/namespaces/media/pods?labelSelector=job-name%3Dembyx-merge-1-x"
    );
    // The last pod the list names is the one asked for its log.
    assert_eq!(
        seen[3].url,
        "https://kube.test/api/v1/namespaces/media/pods/embyx-merge-1-x-abcde/log?tailLines=20"
    );
    assert!(
        seen.iter()
            .all(|request| request.header("Authorization") == Some("Bearer secret-token"))
    );
}

#[tokio::test]
async fn the_job_body_is_compact_utf8_json_and_the_template_is_read_each_time() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![
        Reply::json(
            200,
            &pod(
                "ghcr.io/cyrahs/embyx-manager:latest",
                "docker-pullable://ghcr.io/cyrahs/embyx-manager@sha256:abc",
            ),
        ),
        Reply::json(201, &json!({})),
        Reply::json(201, &json!({})),
    ])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");

    kube.create_job("embyx-merge-1-x", &["--title".to_owned(), "中文".to_owned()], 1)
        .await
        .expect("create");
    std::fs::write(
        mounted.path("job.json"),
        json!({"metadata": {"labels": {"v": "2"}}, "spec": {"template": {"spec": {"containers": [{}]}}}}).to_string(),
    )
    .expect("template");
    kube.create_job("embyx-merge-2-y", &[], 2).await.expect("create");

    let seen = web.seen();
    // The image is looked up once, with `docker-pullable://` dropped.
    assert_eq!(seen.len(), 3);
    assert_eq!(
        std::str::from_utf8(&seen[1].body).expect("utf-8"),
        r#"{"metadata":{"name":"embyx-merge-1-x","labels":{"embyx-manager/merge-task":"1"}},"spec":{"template":{"spec":{"containers":[{"name":"merge","image":"ghcr.io/cyrahs/embyx-manager@sha256:abc","args":["--title","中文"]}]},"metadata":{"labels":{"embyx-manager/merge-task":"1"}}}}}"#
    );
    assert_eq!(
        std::str::from_utf8(&seen[2].body).expect("utf-8"),
        r#"{"metadata":{"labels":{"v":"2","embyx-manager/merge-task":"2"},"name":"embyx-merge-2-y"},"spec":{"template":{"spec":{"containers":[{"image":"ghcr.io/cyrahs/embyx-manager@sha256:abc","args":[]}]},"metadata":{"labels":{"embyx-manager/merge-task":"2"}}}}}"#
    );
}

#[tokio::test]
async fn an_image_without_a_digest_falls_back_to_the_spec() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![Reply::json(
        200,
        &pod("ghcr.io/cyrahs/embyx-manager:latest", "sha256:only"),
    )])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    assert_eq!(
        kube.own_image().await.expect("image"),
        "ghcr.io/cyrahs/embyx-manager:latest"
    );
    // Cached: no second request.
    assert_eq!(
        kube.own_image().await.expect("image"),
        "ghcr.io/cyrahs/embyx-manager:latest"
    );
    assert_eq!(web.seen().len(), 1);

    let web = scripted(vec![Reply::json(
        200,
        &json!({"spec": {"containers": []}, "status": {}}),
    )])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    let error = kube.own_image().await.expect_err("no container");
    assert_eq!(
        (error.class_name(), error.to_string()),
        ("IndexError", "list index out of range".to_owned())
    );
}

#[tokio::test]
async fn refusals_are_kube_errors_with_the_answer_cut_at_300_characters() {
    let mounted = Mounted::new(&minimal_template());
    let long = "x".repeat(400);
    let web = scripted(vec![
        Reply::bytes(403, long.clone()),
        Reply::json(500, &json!({"message": "boom"})),
        Reply::json(404, &json!({})),
        Reply::json(409, &json!({})),
    ])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");

    let error = kube.get_job("a").await.expect_err("403");
    assert!(error.is_kube());
    assert_eq!(
        error.to_string(),
        format!(
            "Kubernetes API GET /apis/batch/v1/namespaces/media/jobs/a answered 403: {}",
            &long[..300]
        )
    );
    let error = kube.delete_job("a").await.expect_err("500");
    assert_eq!(
        error.to_string(),
        r#"Kubernetes API DELETE /apis/batch/v1/namespaces/media/jobs/a answered 500: {"message":"boom"}"#
    );
    // 404 is accepted by delete, 409 is not.
    kube.delete_job("a").await.expect("404");
    let error = kube.delete_job("a").await.expect_err("409");
    assert!(error.to_string().contains("answered 409"));
}

#[tokio::test]
async fn a_success_that_is_not_json_is_a_decode_error_not_a_kube_error() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![Reply::bytes(200, "")]).await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    let error = kube.get_job("a").await.expect_err("empty body");
    assert!(!error.is_kube());
    assert_eq!(error.class_name(), "JSONDecodeError");
    assert_eq!(error.to_string(), "Expecting value: line 1 column 1 (char 0)");
}

#[tokio::test]
async fn log_tail_is_empty_without_a_pod_or_when_the_log_is_refused() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![
        Reply::json(200, &json!({"items": []})),
        Reply::json(200, &json!({})),
        Reply::json(200, &json!({"items": [{"metadata": {"name": "p"}}]})),
        Reply::bytes(400, "container is waiting"),
        Reply::json(403, &json!({"reason": "Forbidden"})),
    ])
    .await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    assert_eq!(kube.log_tail("a", 20).await.expect("no items"), "");
    assert_eq!(kube.log_tail("a", 20).await.expect("no key"), "");
    assert_eq!(kube.log_tail("a", 5).await.expect("refused log"), "");
    assert!(web.seen()[3].url.ends_with("/pods/p/log?tailLines=5"));
    // The pod list itself failing is a KubeError.
    assert!(kube.log_tail("a", 20).await.expect_err("403").is_kube());
}

#[tokio::test]
async fn the_token_is_read_before_every_request() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![Reply::json(404, &json!({})), Reply::json(404, &json!({}))]).await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    kube.get_job("a").await.expect("first");
    std::fs::write(mounted.path("token"), "  rotated-token\n").expect("rotate");
    kube.get_job("a").await.expect("second");
    let seen = web.seen();
    assert_eq!(seen[0].header("Authorization"), Some("Bearer secret-token"));
    assert_eq!(seen[1].header("Authorization"), Some("Bearer rotated-token"));

    std::fs::remove_file(mounted.path("token")).expect("remove");
    let error = kube.get_job("a").await.expect_err("no token");
    assert!(error.is_kube());
    assert_eq!(
        error.to_string(),
        format!(
            "cannot read the service account token: [Errno 2] No such file or directory: '{}'",
            mounted.path("token").display()
        )
    );
    assert_eq!(web.seen().len(), 2);
}

#[tokio::test]
async fn transport_failures_are_kube_errors() {
    let mounted = Mounted::new(&minimal_template());
    for reply in [Reply::Close, Reply::Reset, Reply::Stall(Duration::from_secs(5))] {
        let web = scripted(vec![reply.clone()]).await;
        let mut options = mounted.options(&web);
        options.timeout = Duration::from_millis(300);
        let kube = KubeJobs::new(&options).expect("client");
        let error = kube.get_job("a").await.expect_err("transport failure");
        assert!(error.is_kube(), "{reply:?}: {error:?}");
        assert!(
            error.to_string().starts_with("Kubernetes API unreachable: "),
            "{reply:?}: {error}"
        );
    }
    // A server that does not speak at all.
    let options = KubeOptions {
        api_url: "https://127.0.0.1:1".to_owned(),
        proxy: None,
        ca_path: None,
        ..mounted.options(&scripted(Vec::new()).await)
    };
    let error = KubeJobs::new(&options)
        .expect("client")
        .get_job("a")
        .await
        .expect_err("refused");
    assert!(error.to_string().starts_with("Kubernetes API unreachable: "), "{error}");
}

#[tokio::test]
async fn a_certificate_the_cluster_ca_did_not_sign_is_refused() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![Reply::json(200, &json!({}))]).await;
    // Another authority: the fake web's certificate does not chain to it.
    let other = rcgen::generate_simple_self_signed(vec!["other".to_owned()]).expect("certificate");
    std::fs::write(mounted.path("other.crt"), other.cert.pem()).expect("ca");
    let mut options = mounted.options(&web);
    options.ca_path = Some(mounted.path("other.crt"));
    let error = KubeJobs::new(&options)
        .expect("client")
        .get_job("a")
        .await
        .expect_err("untrusted");
    assert!(error.is_kube());

    std::fs::write(mounted.path("empty.crt"), "").expect("ca");
    options.ca_path = Some(mounted.path("empty.crt"));
    assert_eq!(
        KubeJobs::new(&options).expect_err("no certificate").class_name(),
        "SSLError"
    );
}

#[tokio::test]
async fn template_errors_are_python_errors_not_kube_errors() {
    let mounted = Mounted::new(&json!({"spec": {}}));
    let web = scripted(vec![Reply::json(200, &pod("i", "i@sha256:1"))]).await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    let error = kube.create_job("j", &[], 1).await.expect_err("no template key");
    assert_eq!(
        (error.class_name(), error.to_string()),
        ("KeyError", "'template'".to_owned())
    );

    std::fs::write(mounted.path("job.json"), "{not json").expect("template");
    let error = kube.create_job("j", &[], 1).await.expect_err("bad JSON");
    assert_eq!(error.class_name(), "JSONDecodeError");
    assert_eq!(
        error.to_string(),
        "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
    );
    std::fs::remove_file(mounted.path("job.json")).expect("remove");
    let error = kube.create_job("j", &[], 1).await.expect_err("no template");
    assert_eq!(error.class_name(), "FileNotFoundError");
    assert!(!error.is_kube());
}

#[tokio::test]
async fn a_template_with_nan_is_refused_before_the_request() {
    // httpx refuses `json=` with NaN or infinity (`allow_nan=False`); only the pod lookup is
    // scripted, so a POST would fail as a `KubeError` instead.
    for (literal, shown) in [("NaN", "nan"), ("Infinity", "inf"), ("-Infinity", "-inf")] {
        let mounted = Mounted::new(&minimal_template());
        let template = format!(
            r#"{{"metadata": {{"annotations": {{"x": [1, {literal}]}}}}, "spec": {{"template": {{"spec": {{"containers": [{{}}]}}}}}}}}"#
        );
        std::fs::write(mounted.path("job.json"), template).expect("template");
        let web = scripted(vec![Reply::json(200, &pod("i", "i@sha256:1"))]).await;
        let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
        let error = kube.create_job("j", &[], 1).await.expect_err(literal);
        assert_eq!(
            (error.class_name(), error.to_string()),
            (
                "ValueError",
                format!("Out of range float values are not JSON compliant: {shown}")
            )
        );
    }
}

// ---------- from_environment ----------

fn environment(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let pairs: Vec<(String, String)> = pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    move |name| {
        pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }
}

fn service_account(namespace: Option<&str>, ca: bool) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("token"), "t").expect("token");
    if let Some(namespace) = namespace {
        std::fs::write(dir.path().join("namespace"), namespace).expect("namespace");
    }
    if ca {
        let mounted = Mounted::new(&minimal_template());
        std::fs::copy(mounted.path("ca.crt"), dir.path().join("ca.crt")).expect("ca");
    }
    dir
}

const IN_CLUSTER: [(&str, &str); 3] = [
    ("EMBYX_MANAGER_MERGE_JOB_TEMPLATE", "/etc/embyx/merge-job.json"),
    ("KUBERNETES_SERVICE_HOST", "10.43.0.1"),
    ("POD_NAME", "embyx-manager-0"),
];

#[tokio::test]
async fn from_environment_needs_the_template_the_api_the_pod_and_a_token() {
    let account = service_account(Some(" media\n"), true);
    for missing in [
        "EMBYX_MANAGER_MERGE_JOB_TEMPLATE",
        "KUBERNETES_SERVICE_HOST",
        "POD_NAME",
    ] {
        let pairs: Vec<(&str, &str)> = IN_CLUSTER
            .iter()
            .copied()
            .filter(|(name, _)| *name != missing)
            .collect();
        assert!(
            KubeJobs::from_environment_with(environment(&pairs), account.path())
                .expect("no error")
                .is_none(),
            "{missing}"
        );
        // Set but empty counts as missing.
        let mut empty = pairs.clone();
        empty.push((missing, ""));
        assert!(
            KubeJobs::from_environment_with(environment(&empty), account.path())
                .expect("no error")
                .is_none()
        );
    }
    let without_token = tempfile::tempdir().expect("tempdir");
    assert!(
        KubeJobs::from_environment_with(environment(&IN_CLUSTER), without_token.path())
            .expect("no error")
            .is_none()
    );
    let kube = KubeJobs::from_environment_with(environment(&IN_CLUSTER), account.path())
        .expect("no error")
        .expect("in cluster");
    assert_eq!(&*kube.base_url, "https://10.43.0.1:443");
    assert_eq!(&*kube.namespace, "media");
    assert_eq!(&*kube.pod_name, "embyx-manager-0");
    assert_eq!(&*kube.template_path, Path::new("/etc/embyx/merge-job.json"));
}

#[tokio::test]
async fn from_environment_brackets_ipv6_and_prefers_the_namespace_variable() {
    let account = service_account(None, false);
    let mut pairs = IN_CLUSTER.to_vec();
    pairs[1] = ("KUBERNETES_SERVICE_HOST", "fd00::1");
    pairs.push(("KUBERNETES_SERVICE_PORT", "6443"));
    pairs.push(("POD_NAMESPACE", "other"));
    let kube = KubeJobs::from_environment_with(environment(&pairs), account.path())
        .expect("no error")
        .expect("in cluster");
    assert_eq!(&*kube.base_url, "https://[fd00::1]:6443");
    assert_eq!(&*kube.namespace, "other");

    // Without the variable and without the file, Python's read fails.
    let error = KubeJobs::from_environment_with(environment(&IN_CLUSTER), account.path()).expect_err("no namespace");
    assert_eq!(error.class_name(), "FileNotFoundError");
}

#[test]
fn errors_say_which_python_class_they_stand_for() {
    assert_eq!(KubeJobsError::Kube("x".to_owned()).class_name(), "KubeError");
    assert!(KubeJobsError::Kube("x".to_owned()).is_kube());
    assert!(!KubeJobsError::key("spec").is_kube());
    assert_eq!(TASK_LABEL, "embyx-manager/merge-task");
}

/// Not a port: the `UnicodeDecodeError` text of a template that is not UTF-8, as Python 3.13
/// words it (`b'{"a":"\xc3("}'.decode()` and a truncated sequence), through the shared
/// `http::utf8_error_message` since WP16.
#[tokio::test]
async fn a_template_that_is_not_utf8_reads_as_pythons_unicode_decode_error() {
    let mounted = Mounted::new(&minimal_template());
    let web = scripted(vec![]).await;
    let kube = KubeJobs::new(&mounted.options(&web)).expect("client");
    for (bytes, message) in [
        (
            &b"{\"a\":\"\xc3(\"}"[..],
            "'utf-8' codec can't decode byte 0xc3 in position 6: invalid continuation byte",
        ),
        (
            &b"{\"a\":\"\xff\"}"[..],
            "'utf-8' codec can't decode byte 0xff in position 6: invalid start byte",
        ),
        (
            &b"{\"a\":\"\xe2\x82"[..],
            "'utf-8' codec can't decode bytes in position 6-7: unexpected end of data",
        ),
    ] {
        std::fs::write(mounted.path("job.json"), bytes).expect("template");
        let error = kube.template().expect_err("not UTF-8");
        assert_eq!(error.class_name(), "UnicodeDecodeError");
        assert_eq!(error.to_string(), message);
    }
}
