//! Just enough of the Kubernetes API to run merge Jobs from inside the cluster.
//!
//! Python origin: `src/embyx_manager/merge/kube.py`. The Job's shape lives in the `GitOps`
//! repository as a template the deployment mounts (`EMBYX_MANAGER_MERGE_JOB_TEMPLATE`); this
//! module only names it, points it at the image this pod runs, and passes the worker its
//! arguments. The pod's service account may create, read and delete Jobs and read pods in
//! its own namespace, nothing more.
//!
//! Requests go over HTTPS with the service-account token read from disk before every
//! request (it is rotated), through the httpx-shaped client ([`crate::net::HttpClient`]):
//! 30 s timeouts, no redirects, the cluster CA as the only root when it is mounted. The
//! Job body is the template with four fields set, sent as httpx sends `json=` (compact,
//! UTF-8). Errors keep `kube.py`'s classes and texts: [`KubeJobsError::Kube`] is `KubeError`,
//! which the merge service suppresses around cleanup; everything else (a broken template, a
//! 2xx answer that is not JSON) is another Python class the service does not suppress.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use embyx_core::pyjson::{self, DumpOptions, Value};
use embyx_core::pytext::{py_repr, py_strip};
use embyx_core::pyurl;
use reqwest::Method;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;
use tokio::sync::OnceCell;

use crate::net::client::{ClientOptions, HttpClient, Request};
use crate::net::download::PyOsError;

/// Where Kubernetes mounts the pod's service account.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
/// The environment variable naming the mounted Job template.
pub const TEMPLATE_ENV: &str = "EMBYX_MANAGER_MERGE_JOB_TEMPLATE";
/// `REQUEST_TIMEOUT_SECONDS`.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Job names are DNS labels; the Job controller appends to them for its pods.
pub const MAX_JOB_NAME: usize = 52;
/// The label tying a Job and its pods to their merge task.
pub const TASK_LABEL: &str = "embyx-manager/merge-task";

/// What a Kubernetes call raises, by the Python exception it stands for.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KubeJobsError {
    /// `KubeError`: the Kubernetes API refused or could not be reached.
    #[error("{0}")]
    Kube(String),
    /// Anything else Python lets through: `KeyError`, `IndexError`, `TypeError` from a template
    /// without the fields the Job needs, `JSONDecodeError` from a template or a 2xx answer that
    /// is not JSON, `OSError` from an unreadable template or CA file.
    #[error("{message}")]
    Other { class: &'static str, message: String },
}

impl KubeJobsError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Kube(_) => "KubeError",
            Self::Other { class, .. } => class,
        }
    }

    /// `isinstance(exc, KubeError)`, what `suppress(KubeError)` swallows.
    pub fn is_kube(&self) -> bool {
        matches!(self, Self::Kube(_))
    }

    fn other(class: &'static str, message: impl Into<String>) -> Self {
        Self::Other {
            class,
            message: message.into(),
        }
    }

    fn os(error: &std::io::Error, path: &Path) -> Self {
        let error = PyOsError::from_io(error, Some(path));
        Self::other(error.class, error.message)
    }

    /// `str(KeyError(key))`.
    fn key(key: &str) -> Self {
        Self::other("KeyError", py_repr(key))
    }

    fn index() -> Self {
        Self::other("IndexError", "list index out of range")
    }

    fn not_subscriptable(value: &Value) -> Self {
        Self::other(
            "TypeError",
            format!("'{}' object is not subscriptable", type_name(value)),
        )
    }
}

/// Python's type name of a JSON value.
fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "str",
        Value::List(_) => "list",
        Value::Dict(_) => "dict",
    }
}

/// `job_name(task_id, avid)`: `embyx-merge-<id>-<slug>`, at most 52 characters, never ending
/// in `-`.
pub fn job_name(task_id: i64, avid: &str) -> String {
    let lowered = avid.to_lowercase();
    let mut slug = String::with_capacity(lowered.len());
    let mut in_run = false;
    for ch in lowered.chars() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            slug.push(ch);
            in_run = false;
        } else if !in_run {
            slug.push('-');
            in_run = true;
        }
    }
    let name = format!("embyx-merge-{task_id}-{}", slug.trim_matches('-'));
    // Every character is ASCII by now, so bytes are code points.
    name[..name.len().min(MAX_JOB_NAME)].trim_end_matches('-').to_owned()
}

/// `dict.setdefault(key, {})` on a JSON object; anything else is Python's error.
fn setdefault_object<'a>(value: &'a mut Value, key: &str) -> Result<&'a mut Value, KubeJobsError> {
    let Value::Dict(dict) = value else {
        return Err(KubeJobsError::other(
            "AttributeError",
            format!("'{}' object has no attribute 'setdefault'", type_name(value)),
        ));
    };
    if !dict.contains_key(key) {
        dict.insert(key, Value::Dict(pyjson::Dict::new()));
    }
    Ok(dict.get_mut(key).expect("just inserted"))
}

/// `value[key] = item` on a JSON object.
fn set_item(value: &mut Value, key: &str, item: Value) -> Result<(), KubeJobsError> {
    match value {
        Value::Dict(dict) => {
            dict.insert(key, item);
            Ok(())
        }
        Value::List(_) => Err(KubeJobsError::other(
            "TypeError",
            "list indices must be integers or slices, not str",
        )),
        other => Err(KubeJobsError::other(
            "TypeError",
            format!("'{}' object does not support item assignment", type_name(other)),
        )),
    }
}

/// `value[key]` on a JSON object.
fn get_key<'a>(value: &'a mut Value, key: &str) -> Result<&'a mut Value, KubeJobsError> {
    match value {
        Value::Dict(dict) => dict.get_mut(key).ok_or_else(|| KubeJobsError::key(key)),
        Value::List(_) => Err(KubeJobsError::other(
            "TypeError",
            "list indices must be integers or slices, not str",
        )),
        other => Err(KubeJobsError::not_subscriptable(other)),
    }
}

/// `value[0]`: a list's first item, a string's first character (which then refuses item
/// assignment, as in Python), a dict's key `0`.
fn first_item(value: &mut Value) -> Result<&mut Value, KubeJobsError> {
    match value {
        Value::List(items) => items.first_mut().ok_or_else(KubeJobsError::index),
        Value::Str(text) => {
            let first = text
                .chars()
                .next()
                .ok_or_else(|| KubeJobsError::other("IndexError", "string index out of range"))?;
            *value = Value::Str(first.to_string());
            Ok(value)
        }
        // `{}[0]`: `KeyError(0)` prints the integer.
        Value::Dict(_) => Err(KubeJobsError::other("KeyError", "0")),
        other => Err(KubeJobsError::not_subscriptable(other)),
    }
}

/// `build_job(template, name=..., image=..., args=..., task_id=...)`: the template with this
/// run's name, image, arguments and task label filled in. The template is not changed.
pub fn build_job(
    template: &Value,
    name: &str,
    image: &str,
    args: &[String],
    task_id: i64,
) -> Result<Value, KubeJobsError> {
    let mut job = template.clone();
    let label = Value::from(task_id.to_string());
    let metadata = setdefault_object(&mut job, "metadata")?;
    set_item(metadata, "name", Value::from(name))?;
    set_item(setdefault_object(metadata, "labels")?, TASK_LABEL, label.clone())?;
    let pod = get_key(get_key(&mut job, "spec")?, "template")?;
    set_item(
        setdefault_object(setdefault_object(pod, "metadata")?, "labels")?,
        TASK_LABEL,
        label,
    )?;
    let container = first_item(get_key(get_key(pod, "spec")?, "containers")?)?;
    set_item(container, "image", Value::from(image))?;
    set_item(
        container,
        "args",
        Value::List(args.iter().map(|arg| Value::from(arg.as_str())).collect()),
    )?;
    Ok(job)
}

/// `value.get(key) or default` for a JSON object; a non-object has nothing.
fn get_or_none<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|item| item.is_truthy())
}

fn conditions(job: &Value) -> impl Iterator<Item = &Value> {
    get_or_none(job, "status")
        .and_then(|status| get_or_none(status, "conditions"))
        .and_then(Value::as_list)
        .unwrap_or_default()
        .iter()
}

/// `job_outcome(job)`: `succeeded`, `failed` or `running` from a Job's status.
pub fn job_outcome(job: &Value) -> &'static str {
    for condition in conditions(job) {
        if condition.get("status").and_then(Value::as_str) != Some("True") {
            continue;
        }
        match condition.get("type").and_then(Value::as_str) {
            Some("Complete") => return "succeeded",
            Some("Failed") => return "failed",
            _ => {}
        }
    }
    let status = get_or_none(job, "status");
    if status.and_then(|status| get_or_none(status, "succeeded")).is_some() {
        return "succeeded";
    }
    if status.and_then(|status| get_or_none(status, "failed")).is_some() {
        return "failed";
    }
    "running"
}

/// `str(value)` of a JSON scalar.
fn py_str(value: &Value) -> String {
    match value {
        Value::Str(text) => text.clone(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Null => "None".to_owned(),
        Value::Int(number) => number.to_string(),
        Value::Float(number) => pyjson::float_repr(*number),
        other => pyjson::dumps(other, &DumpOptions::plain()).unwrap_or_default(),
    }
}

/// `job_failure(job)`: why Kubernetes failed the Job, e.g. its deadline passing; empty when
/// it does not say.
pub fn job_failure(job: &Value) -> String {
    for condition in conditions(job) {
        if condition.get("type").and_then(Value::as_str) == Some("Failed")
            && condition.get("status").and_then(Value::as_str) == Some("True")
        {
            return get_or_none(condition, "message")
                .or_else(|| get_or_none(condition, "reason"))
                .map(py_str)
                .unwrap_or_default();
        }
    }
    String::new()
}

/// Where the client finds the API and its credentials.
#[derive(Clone)]
pub struct KubeOptions {
    /// `https://<host>:<port>`.
    pub api_url: String,
    pub namespace: String,
    pub pod_name: String,
    pub template_path: PathBuf,
    pub token_path: PathBuf,
    /// The cluster CA (PEM); `None` keeps the usual roots.
    pub ca_path: Option<PathBuf>,
    /// A proxy every request goes through (tests: the fake web; Python's `transport=`). `None`
    /// leaves the environment's proxy variables in charge, as httpx does.
    pub proxy: Option<String>,
    /// [`REQUEST_TIMEOUT`]; tests shorten it.
    pub timeout: Duration,
}

impl std::fmt::Debug for KubeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeOptions")
            .field("api_url", &self.api_url)
            .field("namespace", &self.namespace)
            .field("pod_name", &self.pod_name)
            .field("template_path", &self.template_path)
            .field("token_path", &self.token_path)
            .field("ca_path", &self.ca_path)
            .field("proxy", &self.proxy.as_ref().map(|_| "<set>"))
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// `KubeJobs`: Jobs, pods and logs in the pod's own namespace.
#[derive(Clone)]
pub struct KubeJobs {
    client: HttpClient,
    base_url: Arc<str>,
    namespace: Arc<str>,
    pod_name: Arc<str>,
    template_path: Arc<Path>,
    token_path: Arc<Path>,
    image: Arc<OnceCell<String>>,
}

impl std::fmt::Debug for KubeJobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeJobs")
            .field("base_url", &self.base_url)
            .field("namespace", &self.namespace)
            .field("pod_name", &self.pod_name)
            .finish_non_exhaustive()
    }
}

/// `urlencode({name: value})` as httpx writes `params=`.
fn query(name: &str, value: &str) -> String {
    let plus = |text: &str| pyurl::quote(text, " ").replace(' ', "+");
    format!("{}={}", plus(name), plus(value))
}

/// The certificates of a PEM file, for `ssl.create_default_context(cafile=...)`.
fn read_ca(path: &Path) -> Result<Vec<Vec<u8>>, KubeJobsError> {
    let pem = std::fs::read(path).map_err(|error| KubeJobsError::os(&error, path))?;
    let roots: Vec<Vec<u8>> = CertificateDer::pem_slice_iter(&pem)
        .filter_map(Result::ok)
        .map(|der| der.as_ref().to_vec())
        .collect();
    if roots.is_empty() {
        // `load_verify_locations` on a file without a certificate.
        return Err(KubeJobsError::other(
            "SSLError",
            "[X509: NO_CERTIFICATE_OR_CRL_FOUND] no certificate or crl found",
        ));
    }
    Ok(roots)
}

impl KubeJobs {
    /// `KubeJobs(api_url=..., namespace=..., pod_name=..., template_path=..., token_path=...,
    /// ca_path=...)`.
    pub fn new(options: &KubeOptions) -> Result<Self, KubeJobsError> {
        let roots = match &options.ca_path {
            Some(path) => read_ca(path)?,
            None => Vec::new(),
        };
        let client = HttpClient::new(&ClientOptions {
            timeout: options.timeout,
            proxy: options.proxy.clone(),
            roots,
            ..ClientOptions::default()
        })
        .map_err(|error| KubeJobsError::other("ValueError", error.to_string()))?;
        Ok(Self {
            client,
            base_url: Arc::from(options.api_url.trim_end_matches('/')),
            namespace: Arc::from(options.namespace.as_str()),
            pod_name: Arc::from(options.pod_name.as_str()),
            template_path: Arc::from(options.template_path.as_path()),
            token_path: Arc::from(options.token_path.as_path()),
            image: Arc::new(OnceCell::new()),
        })
    }

    /// `KubeJobs.from_environment()`: the in-cluster client, or `None` outside a cluster or
    /// without a mounted template.
    pub fn from_environment() -> Result<Option<Self>, KubeJobsError> {
        Self::from_environment_with(|name| std::env::var(name).ok(), Path::new(SERVICE_ACCOUNT_DIR))
    }

    /// [`Self::from_environment`] with the environment and the service-account directory
    /// given (tests).
    pub fn from_environment_with(
        env: impl Fn(&str) -> Option<String>,
        service_account: &Path,
    ) -> Result<Option<Self>, KubeJobsError> {
        let present = |name: &str| env(name).filter(|value| !value.is_empty());
        let template = present(TEMPLATE_ENV);
        let host = present("KUBERNETES_SERVICE_HOST");
        let port = env("KUBERNETES_SERVICE_PORT").unwrap_or_else(|| "443".to_owned());
        let pod_name = present("POD_NAME");
        let token_path = service_account.join("token");
        let (Some(template), Some(host), Some(pod_name)) = (template, host, pod_name) else {
            return Ok(None);
        };
        if !token_path.exists() {
            return Ok(None);
        }
        let namespace = if let Some(namespace) = present("POD_NAMESPACE") {
            namespace
        } else {
            let path = service_account.join("namespace");
            let text = std::fs::read_to_string(&path).map_err(|error| KubeJobsError::os(&error, &path))?;
            py_strip(&text).to_owned()
        };
        let host = if host.contains(':') { format!("[{host}]") } else { host };
        let ca_path = service_account.join("ca.crt");
        Self::new(&KubeOptions {
            api_url: format!("https://{host}:{port}"),
            namespace,
            pod_name,
            template_path: PathBuf::from(template),
            token_path,
            ca_path: ca_path.exists().then_some(ca_path),
            proxy: None,
            timeout: REQUEST_TIMEOUT,
        })
        .map(Some)
    }

    /// `template()`: read on every Job so a template change applies without a restart.
    pub fn template(&self) -> Result<Value, KubeJobsError> {
        let bytes =
            std::fs::read(&self.template_path).map_err(|error| KubeJobsError::os(&error, &self.template_path))?;
        let text = String::from_utf8(bytes).map_err(|error| {
            KubeJobsError::other(
                "UnicodeDecodeError",
                format!(
                    "'utf-8' codec can't decode byte 0x{:02x} in position {}: invalid start byte",
                    error.as_bytes()[error.utf8_error().valid_up_to()],
                    error.utf8_error().valid_up_to()
                ),
            )
        })?;
        pyjson::loads(&text).map_err(|error| KubeJobsError::other("JSONDecodeError", error.python_message(&text)))
    }

    /// `own_image()`: the exact image this pod runs, by digest, so the Job runs the same code.
    pub async fn own_image(&self) -> Result<String, KubeJobsError> {
        self.image
            .get_or_try_init(|| async {
                let path = format!("/api/v1/namespaces/{}/pods/{}", self.namespace, self.pod_name);
                let pod = self.request(Method::GET, &path, None, &[], None).await?;
                let pod = pod.unwrap_or(Value::Null);
                let image_id = get_or_none(&pod, "status")
                    .and_then(|status| get_or_none(status, "containerStatuses"))
                    .and_then(Value::as_list)
                    .and_then(<[Value]>::first)
                    .map(|first| first.get("imageID").map(py_str).unwrap_or_default())
                    .unwrap_or_default();
                let image_id = image_id
                    .strip_prefix("docker-pullable://")
                    .map_or(image_id.clone(), str::to_owned);
                if image_id.contains("@sha256:") {
                    return Ok(image_id);
                }
                let mut pod = pod;
                let image = get_key(first_item(get_key(get_key(&mut pod, "spec")?, "containers")?)?, "image")?;
                Ok(py_str(image))
            })
            .await
            .cloned()
    }

    /// `create_job(name=..., args=..., task_id=...)`: start the Job; one that already exists
    /// under the name counts as started.
    pub async fn create_job(&self, name: &str, args: &[String], task_id: i64) -> Result<(), KubeJobsError> {
        let template = self.template()?;
        let image = self.own_image().await?;
        let job = build_job(&template, name, &image, args, task_id)?;
        let path = format!("/apis/batch/v1/namespaces/{}/jobs", self.namespace);
        self.request(Method::POST, &path, Some(&job), &[409], None).await?;
        Ok(())
    }

    /// `get_job(name)`: the Job, or `None` when there is none.
    pub async fn get_job(&self, name: &str) -> Result<Option<Value>, KubeJobsError> {
        let path = format!("/apis/batch/v1/namespaces/{}/jobs/{name}", self.namespace);
        self.request(Method::GET, &path, None, &[404], None).await
    }

    /// `delete_job(name)`: delete it and, in the background, its pods; a missing Job is fine.
    pub async fn delete_job(&self, name: &str) -> Result<(), KubeJobsError> {
        let path = format!("/apis/batch/v1/namespaces/{}/jobs/{name}", self.namespace);
        let mut body = pyjson::Dict::new();
        body.insert("propagationPolicy", Value::from("Background"));
        self.request(Method::DELETE, &path, Some(&Value::Dict(body)), &[404], None)
            .await?;
        Ok(())
    }

    /// `log_tail(name, lines=...)`: the last lines the Job's pod printed, or empty when there
    /// is no pod to ask (Python's default is 20).
    pub async fn log_tail(&self, name: &str, lines: u32) -> Result<String, KubeJobsError> {
        let path = format!("/api/v1/namespaces/{}/pods", self.namespace);
        let selector = format!("job-name={name}");
        let pods = self
            .request(Method::GET, &path, None, &[], Some(("labelSelector", &selector)))
            .await?
            .unwrap_or(Value::Null);
        let Some(mut pod) = get_or_none(&pods, "items")
            .and_then(Value::as_list)
            .and_then(<[Value]>::last)
            .cloned()
        else {
            return Ok(String::new());
        };
        let pod = py_str(get_key(get_key(&mut pod, "metadata")?, "name")?);
        let path = format!("/api/v1/namespaces/{}/pods/{pod}/log", self.namespace);
        let lines = lines.to_string();
        let (status, body) = self.send(Method::GET, &path, None, Some(("tailLines", &lines))).await?;
        Ok(if (200..300).contains(&status) {
            embyx_core::pytext::py_strip(&String::from_utf8_lossy(&body)).to_owned()
        } else {
            String::new()
        })
    }

    /// `_send`: one request with a freshly read token.
    async fn send(
        &self,
        method: Method,
        path: &str,
        json: Option<&Value>,
        params: Option<(&str, &str)>,
    ) -> Result<(u16, Vec<u8>), KubeJobsError> {
        let token = std::fs::read_to_string(&self.token_path).map_err(|error| {
            KubeJobsError::Kube(format!(
                "cannot read the service account token: {}",
                PyOsError::from_io(&error, Some(&self.token_path)).message
            ))
        })?;
        let mut url = format!("{}{path}", self.base_url);
        if let Some((name, value)) = params {
            url = format!("{url}?{}", query(name, value));
        }
        let mut request = Request::new(method, url).header("Authorization", format!("Bearer {}", py_strip(&token)));
        if let Some(json) = json {
            let body = pyjson::dumps(json, &DumpOptions::compact())
                .map_err(|error| KubeJobsError::other("ValueError", error.to_string()))?;
            request = request.header("Content-Type", "application/json");
            request.body = Some(body.into_bytes());
        }
        let unreachable = |message: &str| KubeJobsError::Kube(format!("Kubernetes API unreachable: {message}"));
        let response = self
            .client
            .send(request)
            .await
            .map_err(|error| unreachable(&error.message))?;
        let status = response.status();
        let body = response.bytes().await.map_err(|error| unreachable(&error.message))?;
        Ok((status, body.to_vec()))
    }

    /// `_request`: `None` for an accepted status, the JSON answer for a success, `KubeError`
    /// for anything else.
    async fn request(
        &self,
        method: Method,
        path: &str,
        json: Option<&Value>,
        accept: &[u16],
        params: Option<(&str, &str)>,
    ) -> Result<Option<Value>, KubeJobsError> {
        let method_name = method.as_str().to_owned();
        let (status, body) = self.send(method, path, json, params).await?;
        if accept.contains(&status) {
            return Ok(None);
        }
        let text = String::from_utf8_lossy(&body);
        if !(200..300).contains(&status) {
            let head: String = text.chars().take(300).collect();
            return Err(KubeJobsError::Kube(format!(
                "Kubernetes API {method_name} {path} answered {status}: {head}"
            )));
        }
        pyjson::loads(&text)
            .map(Some)
            .map_err(|error| KubeJobsError::other("JSONDecodeError", error.python_message(&text)))
    }
}

#[cfg(test)]
mod tests;
