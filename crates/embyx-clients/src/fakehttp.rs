//! The scripted HTTP fake of the effect transcripts: the stand-in for Kubernetes and Emby.
//!
//! Port of `tools/scenarios/fakes/http.py`, rule for rule (docs/rewrite/SCENARIOS.md,
//! "Fakes"). Routes come from the scenario seed, `{method, path, status, body, times}`; the
//! first route whose method and path match and that is not used up answers (and is used up
//! after `times` answers, if given). An unmatched request answers 404 with
//! `{"error": "no route"}`. Each call is recorded as `{fake, method, request: {path, query,
//! body}, response: {status, body}}`: the query as `parse_qs` gives it, a JSON body parsed,
//! another body kept as text, no body `null`. The answer is the route's body as
//! `json.dumps(body, ensure_ascii=False)`, or nothing when it has none.
//!
//! A seed with a `jobs` table adds the Kubernetes Jobs model of WP14 behind the routes, rule
//! for rule as `fakes/http.py` has it: create (201 with an empty `status`, 409 for a name in
//! use), read and delete by name (404 `{"reason": "NotFound"}` when unknown), and
//! [`FakeHttp::set_job_status`] for the cluster finishing a Job; the dump then has `jobs`.
//!
//! Like Python's `http.server`, it answers in HTTP/1.0 and closes the connection after each
//! answer.

use std::sync::{Arc, Mutex};

use embyx_core::pyjson::{self, Dict, DumpOptions, Value};
use embyx_core::pyurl;
use embyx_core::scenario::Recorder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// `_parse_body(raw)`: JSON if it parses, else the text (UTF-8 with replacement).
fn parse_body(raw: &[u8]) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    let text = String::from_utf8_lossy(raw).into_owned();
    pyjson::loads(&text).unwrap_or(Value::Str(text))
}

/// `parse_qs(query)`: names in first-seen order, each with its values; pairs without `=`
/// or with an empty value are dropped.
fn parse_qs(query: &str) -> Value {
    let unquote_plus = |text: &str| pyurl::unquote(&text.replace('+', " "));
    let mut parsed = Dict::new();
    for pair in query.split('&') {
        let Some((name, value)) = pair.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        let name = unquote_plus(name);
        let value = Value::from(unquote_plus(value));
        match parsed.get_mut(&name) {
            Some(Value::List(values)) => values.push(value),
            _ => parsed.insert(name.as_str(), Value::List(vec![value])),
        }
    }
    Value::Dict(parsed)
}

fn times(route: &Value) -> Option<i128> {
    match route.get("times") {
        None => None,
        Some(Value::Int(times)) => Some(*times),
        Some(other) => panic!("times must be an integer, not {other:?}"),
    }
}

/// The Jobs model's store, by name; `None` without a `jobs` seed.
type Jobs = Arc<Mutex<Option<Dict>>>;

fn reason_body(key: &str, text: &str) -> Value {
    let mut body = Dict::new();
    body.insert(key, Value::from(text));
    Value::Dict(body)
}

/// `FakeHttpServer._model`: the Jobs model's answer, or the 404 of a request nothing answers.
fn model(jobs: &mut Option<Dict>, method: &str, path: &str, body: &Value) -> (u16, Value) {
    let no_route = || (404, reason_body("error", "no route"));
    let Some(jobs) = jobs.as_mut() else {
        return no_route();
    };
    let Some(rest) = path.strip_prefix("/apis/batch/v1/namespaces/") else {
        return no_route();
    };
    let Some((namespace, rest)) = rest.split_once('/') else {
        return no_route();
    };
    if namespace.is_empty() {
        return no_route();
    }
    let name = match rest {
        "jobs" => None,
        _ => match rest.strip_prefix("jobs/") {
            Some(name) if !name.is_empty() && !name.contains('/') => Some(name),
            _ => return no_route(),
        },
    };
    match (name, method, body) {
        (None, "POST", Value::Dict(fields)) => {
            let created = match fields.get("metadata").and_then(|metadata| metadata.get("name")) {
                Some(Value::Str(name)) => name.clone(),
                Some(other) => crate::pyvalue::py_str(other),
                None => String::new(),
            };
            if jobs.get(&created).is_some() {
                return (409, reason_body("reason", "AlreadyExists"));
            }
            let mut job = fields.clone();
            job.insert("status", Value::Dict(Dict::new()));
            jobs.insert(created.as_str(), Value::Dict(job.clone()));
            (201, Value::Dict(job))
        }
        (Some(name), "GET", _) => jobs
            .get(name)
            .map_or_else(|| (404, reason_body("reason", "NotFound")), |job| (200, job.clone())),
        (Some(name), "DELETE", _) => {
            if jobs.remove(name).is_none() {
                return (404, reason_body("reason", "NotFound"));
            }
            (200, reason_body("status", "Success"))
        }
        _ => no_route(),
    }
}

/// One fake HTTP service on a local port.
#[derive(Clone)]
pub struct FakeHttp {
    name: Arc<str>,
    routes: Arc<Mutex<Vec<Value>>>,
    jobs: Jobs,
    /// `http://127.0.0.1:<port>`.
    pub url: String,
    task: Arc<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for FakeHttp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeHttp")
            .field("name", &self.name)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Drop for FakeHttp {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task) == 1 {
            self.task.abort();
        }
    }
}

impl FakeHttp {
    /// `FakeHttpServer(name, seed, record)`, started.
    pub async fn start(name: &str, seed: &Value, recorder: Recorder) -> Self {
        let routes: Vec<Value> = seed
            .get("routes")
            .and_then(Value::as_list)
            .map(<[Value]>::to_vec)
            .unwrap_or_default();
        let routes = Arc::new(Mutex::new(routes));
        let jobs: Jobs = Arc::new(Mutex::new(match seed.get("jobs") {
            Some(Value::Dict(jobs)) => Some(jobs.clone()),
            Some(other) => panic!("jobs must be a table, not {other:?}"),
            None => None,
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let name: Arc<str> = Arc::from(name);
        let (served_name, served_routes, served_jobs) = (Arc::clone(&name), Arc::clone(&routes), Arc::clone(&jobs));
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (name, routes, jobs, recorder) = (
                    Arc::clone(&served_name),
                    Arc::clone(&served_routes),
                    Arc::clone(&served_jobs),
                    recorder.clone(),
                );
                tokio::spawn(async move { handle(stream, &name, &routes, &jobs, &recorder).await });
            }
        });
        Self {
            name,
            routes,
            jobs,
            url,
            task: Arc::new(task),
        }
    }

    /// `dump()`: the routes not used up, as they stand.
    pub fn dump(&self) -> Value {
        let left = self
            .routes
            .lock()
            .expect("routes")
            .iter()
            .filter(|route| times(route) != Some(0))
            .cloned()
            .collect();
        let mut dump = Dict::new();
        dump.insert("routes_left", Value::List(left));
        if let Some(jobs) = self.jobs.lock().expect("jobs").as_ref() {
            dump.insert("jobs", Value::Dict(jobs.clone()));
        }
        Value::Dict(dump)
    }

    /// `set_job_status(name, status)`: the cluster changed a kept Job's status.
    pub fn set_job_status(&self, name: &str, status: &Value) {
        let mut jobs = self.jobs.lock().expect("jobs");
        let job = jobs
            .as_mut()
            .and_then(|jobs| jobs.get_mut(name))
            .unwrap_or_else(|| panic!("no Job {name:?} to set the status of"));
        let Value::Dict(job) = job else {
            panic!("a kept Job is an object");
        };
        job.insert("status", status.clone());
    }
}

/// `BaseHTTPRequestHandler`'s reason phrases (`http.HTTPStatus`).
fn reason(status: u16) -> &'static str {
    http::StatusCode::from_u16(status)
        .ok()
        .and_then(|status| status.canonical_reason())
        .unwrap_or("")
}

async fn handle(
    mut stream: TcpStream,
    name: &str,
    routes: &Mutex<Vec<Value>>,
    jobs: &Mutex<Option<Dict>>,
    recorder: &Recorder,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    let head_end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut body = buffer[head_end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split(' ');
    let method = request_line.next().unwrap_or_default().to_owned();
    let target = request_line.next().unwrap_or_default().to_owned();
    let length: usize = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(header, _)| header.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0);
    while body.len() < length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }
    body.truncate(length);
    let (path, query) = match target
        .split_once('#')
        .map_or(target.as_str(), |(before, _)| before)
        .split_once('?')
    {
        Some((path, query)) => (path.to_owned(), query.to_owned()),
        None => (target.clone(), String::new()),
    };
    let request_body = if length > 0 { parse_body(&body) } else { Value::Null };
    let (status, answer) = {
        let mut routes = routes.lock().expect("routes");
        let route = routes.iter_mut().find(|route| {
            route.get("method").and_then(Value::as_str) == Some(method.as_str())
                && route.get("path").and_then(Value::as_str) == Some(path.as_str())
                && times(route) != Some(0)
        });
        let (status, answer) = match route {
            None => model(&mut jobs.lock().expect("jobs"), &method, &path, &request_body),
            Some(route) => {
                if let Some(left) = times(route)
                    && let Value::Dict(route) = route
                {
                    route.insert("times", Value::Int(left - 1));
                }
                let status = match route.get("status") {
                    None => 200,
                    Some(Value::Int(status)) => u16::try_from(*status).expect("an HTTP status"),
                    Some(other) => panic!("status must be an integer, not {other:?}"),
                };
                (status, route.get("body").cloned().unwrap_or(Value::Null))
            }
        };
        let mut request = Dict::new();
        request.insert("path", Value::from(path.as_str()));
        request.insert("query", parse_qs(&query));
        request.insert("body", request_body);
        let mut response = Dict::new();
        response.insert("status", Value::Int(i128::from(status)));
        response.insert("body", answer.clone());
        let mut record = Dict::new();
        record.insert("fake", Value::from(name));
        record.insert("method", Value::from(method.as_str()));
        record.insert("request", Value::Dict(request));
        record.insert("response", Value::Dict(response));
        recorder.record(Value::Dict(record));
        (status, answer)
    };
    let payload = if matches!(answer, Value::Null) {
        Vec::new()
    } else {
        pyjson::dumps(&answer, &DumpOptions::plain())
            .expect("a route body dumps")
            .into_bytes()
    };
    let head = format!(
        "HTTP/1.0 {status} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
        reason(status),
        payload.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&payload).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use embyx_core::pyjson::{self, DumpOptions};

    use super::parse_qs;

    #[test]
    fn query_parses_as_parse_qs() {
        let parsed = parse_qs("labelSelector=job-name%3Dembyx-merge-1-a+b&x=&y&tailLines=20&tailLines=5");
        assert_eq!(
            pyjson::dumps(&parsed, &DumpOptions::plain()).expect("dumps"),
            r#"{"labelSelector": ["job-name=embyx-merge-1-a b"], "tailLines": ["20", "5"]}"#
        );
        assert_eq!(
            pyjson::dumps(&parse_qs(""), &DumpOptions::plain()).expect("dumps"),
            "{}"
        );
    }
}
