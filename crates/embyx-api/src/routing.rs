//! Routes and the dispatcher that runs them in Starlette's order.
//!
//! Python origin: `starlette.routing.Router.app` and `Route.matches` / `Route.handle`
//! (Starlette 1.6.0), `fastapi.FastAPI.include_router`, and the middleware stack
//! `create_app` builds: `ServerErrorMiddleware`, then `add_api_cache_control` (no-store under
//! `/api/`), then `RequestSizeLimitMiddleware`, then the router.
//!
//! The router walks its routes in order. A route whose path matches and whose method is
//! allowed answers; a route whose path matches with another method is remembered (the
//! first one only). The SPA mount, last in the list, matches every path, so with a front end
//! every unmatched request goes to the static files. Without one, a remembered route
//! answers 405 with `Allow`, a path that would match with its trailing slash toggled is
//! redirected (307), and the rest is 404.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use embyx_core::pyurl::quote;
use futures_util::future::BoxFuture;
use http::header::{ALLOW, CACHE_CONTROL, HeaderValue, LOCATION};
use http::{Method, StatusCode};
use http_body_util::BodyExt as _;

use crate::http::{Answer, Request, RequestHead, Response, detail, error_envelope, json, with_body};
use crate::static_files::SpaStaticFiles;

/// A route's body: takes the request (path parameters filled in) and answers.
pub type Handler = Arc<dyn Fn(Request) -> BoxFuture<'static, Answer> + Send + Sync>;

/// Wrap an async function as a [`Handler`].
pub fn handler<F, Fut>(function: F) -> Handler
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Answer> + Send + 'static,
{
    Arc::new(move |request| Box::pin(function(request)))
}

/// One path operation.
#[derive(Clone)]
pub struct Route {
    /// The full path template (`/api/config/{section}`). A `{name}` segment matches one
    /// non-empty path segment (Starlette's `str` convertor, `[^/]+`).
    pub path: String,
    /// The methods it answers. FastAPI's `APIRoute` adds no implicit `HEAD`; Starlette's
    /// plain `Route` (the OpenAPI document's) does, so that route lists it.
    pub methods: Vec<Method>,
    pub handler: Handler,
    /// The operation object of `/openapi.json` (JSON text), or `None` when the route is not
    /// in the schema (`include_in_schema=False`).
    pub openapi: Option<&'static str>,
}

impl std::fmt::Debug for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Route")
            .field("path", &self.path)
            .field("methods", &self.methods)
            .finish_non_exhaustive()
    }
}

impl Route {
    /// `Route.matches` on the path alone: the parameters when it matches.
    pub fn match_path(&self, path: &str) -> Option<Vec<(&'static str, String)>> {
        match_template(&self.path, path)
    }

    /// The `Allow` header of the 405 a method mismatch gets: `', '.join(methods)`.
    fn allow(&self) -> String {
        self.methods.iter().map(Method::as_str).collect::<Vec<_>>().join(", ")
    }
}

/// Match `path` against a template whose `{name}` segments are parameters. The parameter
/// names are leaked once per template, so they can live in the request.
fn match_template(template: &str, path: &str) -> Option<Vec<(&'static str, String)>> {
    let mut wanted = template.split('/');
    let mut given = path.split('/');
    let mut params = Vec::new();
    loop {
        match (wanted.next(), given.next()) {
            (None, None) => return Some(params),
            (Some(want), Some(got)) => {
                if let Some(name) = want.strip_prefix('{').and_then(|rest| rest.strip_suffix('}')) {
                    if got.is_empty() {
                        return None;
                    }
                    params.push((param_name(name), got.to_owned()));
                } else if want != got {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

/// A parameter name as a `&'static str`, interned so each name is leaked once.
fn param_name(name: &str) -> &'static str {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(known) = names.get(name) {
        return known;
    }
    let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
    names.insert(leaked);
    leaked
}

/// `APIRouter(prefix=...)`: the routes one feature contributes.
#[derive(Debug, Clone, Default)]
pub struct ApiRouter {
    prefix: String,
    routes: Vec<Route>,
}

impl ApiRouter {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_owned(),
            routes: Vec::new(),
        }
    }

    /// `@router.<method>(path)`.
    #[must_use]
    pub fn route(mut self, method: Method, path: &str, openapi: Option<&'static str>, handler: Handler) -> Self {
        self.routes.push(Route {
            path: format!("{}{path}", self.prefix),
            methods: vec![method],
            handler,
            openapi,
        });
        self
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub fn into_routes(self) -> Vec<Route> {
        self.routes
    }
}

/// Which of `create_app`'s layers an app has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layers {
    /// `RequestSizeLimitMiddleware(max_bytes)` on POST, PUT and PATCH.
    pub max_request_bytes: Option<usize>,
    /// `add_api_cache_control`: `Cache-Control: no-store` on every answer under `/api/`.
    pub api_no_store: bool,
}

/// An assembled application: its routes in order, the SPA mount (if any) and its layers.
#[derive(Clone)]
pub struct App {
    inner: Arc<AppInner>,
}

struct AppInner {
    routes: Vec<Route>,
    frontend: Option<SpaStaticFiles>,
    layers: Layers,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("routes", &self.inner.routes)
            .field("frontend", &self.inner.frontend.is_some())
            .field("layers", &self.inner.layers)
            .finish()
    }
}

/// The request methods `RequestSizeLimitMiddleware` reads ahead.
fn limited(method: &Method) -> bool {
    *method == Method::POST || *method == Method::PUT || *method == Method::PATCH
}

/// `request.url.path`: the decoded path as `urlsplit` reads it back out of the URL Starlette
/// builds, so a decoded `?` or `#` (`%3F`, `%23`) ends it.
pub fn url_path(path: &str) -> &str {
    path.find(['?', '#']).map_or(path, |end| &path[..end])
}

impl App {
    pub fn new(routes: Vec<Route>, frontend: Option<SpaStaticFiles>, layers: Layers) -> Self {
        Self {
            inner: Arc::new(AppInner {
                routes,
                frontend,
                layers,
            }),
        }
    }

    pub fn routes(&self) -> &[Route] {
        &self.inner.routes
    }

    /// One request through the whole stack. Never fails: what nothing handled is logged
    /// and answered with a plain 500 (`ServerErrorMiddleware`), without `no-store`.
    pub async fn call(&self, request: http::Request<Body>, server: Option<SocketAddr>) -> Response {
        let request = Request::from_http(request, server);
        let head = request.method == Method::HEAD;
        let under_api = url_path(&request.path).starts_with("/api/");
        let mut response = match self.limit_size(request).await {
            Ok(mut response) => {
                if self.inner.layers.api_no_store && under_api {
                    response
                        .headers_mut()
                        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
                }
                response
            }
            Err(error) => error.into_response(),
        };
        if head {
            // The server sends no body for HEAD; the headers (content-length too) stay.
            *response.body_mut() = Body::empty();
        }
        response
    }

    /// `RequestSizeLimitMiddleware`: read a POST, PUT or PATCH body ahead and refuse it with
    /// 413 as soon as more than `max_bytes` arrived; otherwise hand it on.
    async fn limit_size(&self, mut request: Request) -> Answer {
        let Some(max_bytes) = self.inner.layers.max_request_bytes else {
            return self.route(request).await;
        };
        if !limited(&request.method) {
            return self.route(request).await;
        }
        let mut body = std::mem::replace(&mut request.body, Body::empty());
        let mut received = BytesMut::new();
        let mut disconnected = false;
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else {
                // `http.disconnect`: what arrived is replayed, then the body ends in a
                // disconnect for whoever reads it.
                disconnected = true;
                break;
            };
            if let Ok(data) = frame.into_data() {
                received.extend_from_slice(&data);
                if received.len() > max_bytes {
                    return Ok(json(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        &error_envelope("request_too_large"),
                    ));
                }
            }
        }
        request.body = if disconnected {
            disconnected_body(received.freeze())
        } else {
            Body::from(received.freeze())
        };
        self.route(request).await
    }

    /// `Router.app`.
    async fn route(&self, mut request: Request) -> Answer {
        let mut partial: Option<&Route> = None;
        for route in &self.inner.routes {
            if let Some(params) = route.match_path(&request.path) {
                if route.methods.contains(&request.method) {
                    request.params = params;
                    return (route.handler)(request).await;
                }
                partial.get_or_insert(route);
            }
        }
        if let Some(frontend) = &self.inner.frontend {
            // `app.mount('/', SpaStaticFiles(...))`: last, and it matches every path.
            return frontend.serve(&request.head()).await;
        }
        if let Some(route) = partial {
            return Ok(detail(StatusCode::METHOD_NOT_ALLOWED, &[(ALLOW, route.allow())]));
        }
        if request.path != "/" {
            let toggled = if request.path.ends_with('/') {
                request.path.trim_end_matches('/').to_owned()
            } else {
                format!("{}/", request.path)
            };
            if self
                .inner
                .routes
                .iter()
                .any(|route| route.match_path(&toggled).is_some())
            {
                return Ok(redirect(&absolute_url(&request.head(), &toggled)));
            }
        }
        Ok(detail(StatusCode::NOT_FOUND, &[]))
    }
}

/// A body that yields what was received and then fails, as a disconnected client's does.
fn disconnected_body(received: Bytes) -> Body {
    let chunks = futures_util::stream::iter([
        Ok(received),
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "client disconnected",
        )),
    ]);
    Body::from_stream(chunks)
}

/// `starlette.datastructures._HOST_RE`: the `Host` header is used for absolute URLs only
/// when it is a plain host name or bracketed IPv6 address with an optional port.
fn valid_host(host: &str) -> bool {
    fn port(rest: &str) -> bool {
        rest.is_empty()
            || rest
                .strip_prefix(':')
                .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
    }
    if let Some(bracketed) = host.strip_prefix('[') {
        let Some((inside, rest)) = bracketed.split_once(']') else {
            return false;
        };
        // `\[[a-f0-9]*:[a-f0-9.:]+\]`
        let Some((before, after)) = inside.split_once(':') else {
            return false;
        };
        return before.bytes().all(|byte| byte.is_ascii_hexdigit())
            && !after.is_empty()
            && after
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'.' || byte == b':')
            && port(rest);
    }
    // `[a-z0-9.-]+(?::[0-9]+)?`: the name stops at the first character outside the class.
    let end = host
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '.' || ch == '-'))
        .unwrap_or(host.len());
    end > 0 && port(&host[end..])
}

/// `str(URL(scope=scope))` with the path replaced: `http://<netloc><path>[?<query>]`. The
/// netloc is the `Host` header when it is valid, else the listener's own address.
pub fn absolute_url(request: &RequestHead, path: &str) -> String {
    let host_header = request.header("host");
    let netloc = match (&host_header, request.server) {
        (Some(host), _) if valid_host(host) => Some(host.clone()),
        (_, Some(server)) => Some(if server.port() == 80 {
            server.ip().to_string()
        } else {
            format!("{}:{}", server.ip(), server.port())
        }),
        _ => None,
    };
    let query = if request.query.is_empty() {
        String::new()
    } else {
        format!("?{}", request.query)
    };
    match netloc {
        Some(netloc) => format!("http://{netloc}{path}{query}"),
        None => format!("{path}{query}"),
    }
}

/// `RedirectResponse(url)`: 307, the URL quoted with Starlette's safe set, no body.
pub fn redirect(url: &str) -> Response {
    let mut response = with_body(StatusCode::TEMPORARY_REDIRECT, None, Bytes::new());
    let location = quote(url, ":/%#?=@[]!$&'()*+,;");
    response.headers_mut().insert(
        LOCATION,
        HeaderValue::from_str(&location).expect("a quoted URL is a valid header"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::{match_template, url_path, valid_host};

    #[test]
    fn templates_match_whole_segments() {
        assert_eq!(match_template("/api/config", "/api/config"), Some(vec![]));
        assert_eq!(match_template("/api/config", "/api/config/"), None);
        assert_eq!(
            match_template("/api/config/{section}", "/api/config/emby"),
            Some(vec![("section", "emby".to_owned())])
        );
        assert_eq!(match_template("/api/config/{section}", "/api/config/"), None);
        assert_eq!(match_template("/api/config/{section}", "/api/config/a/b"), None);
        assert_eq!(
            match_template("/api/config/{section}", "/api/config/\u{fffd}"),
            Some(vec![("section", "\u{fffd}".to_owned())])
        );
    }

    #[test]
    fn host_headers_are_checked_like_starlette() {
        for good in [
            "testserver",
            "a.b-c:8080",
            "127.0.0.1:80",
            "[::1]",
            "[::1]:8080",
            "[fe80::1]",
            "EXAMPLE.com",
        ] {
            assert!(valid_host(good), "{good}");
        }
        for bad in [
            "", "a b", "a:b", "a:", "[::1", "[]", "[1]", "[:]", "a/b", "a@b", "\u{e9}",
        ] {
            assert!(!valid_host(bad), "{bad}");
        }
    }

    #[test]
    fn the_url_path_ends_at_a_decoded_query_or_fragment() {
        assert_eq!(url_path("/api/x"), "/api/x");
        assert_eq!(url_path("/api?/x"), "/api");
        assert_eq!(url_path("/a#/api/"), "/a");
    }
}
