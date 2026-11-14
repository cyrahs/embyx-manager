//! The bearer token every mutation endpoint shares.
//!
//! Python origin: `api.py` (`make_mutation_auth`) over FastAPI's `HTTPBearer(auto_error=False)`
//! (`fastapi/security/http.py`, `get_authorization_scheme_param`). The first `Authorization`
//! header is split at its first space; the part after it, stripped of Python whitespace, is
//! the credential; the scheme must be `bearer` in any case. The comparison is constant time
//! (`secrets.compare_digest`).

use std::sync::Arc;

use embyx_core::pytext::py_strip;
use http::StatusCode;
use http::header::WWW_AUTHENTICATE;

use crate::http::{Answer, Internal, Request, Response, error_envelope, json};

/// `ApiError(401, 'unauthorized')` as `create_app`'s handler renders it: the envelope plus
/// `WWW-Authenticate: Bearer`.
pub fn unauthorized() -> Response {
    let mut response = json(StatusCode::UNAUTHORIZED, &error_envelope("unauthorized"));
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, http::HeaderValue::from_static("Bearer"));
    response
}

/// `HTTPBearer(auto_error=False)(request)`: the credential of a `Bearer` header, or `None`.
pub fn bearer_credentials(request: &Request) -> Option<String> {
    let authorization = request.header("authorization")?;
    if authorization.is_empty() {
        return None;
    }
    let (scheme, param) = authorization.split_once(' ').unwrap_or((authorization.as_str(), ""));
    let credentials = py_strip(param);
    if scheme.is_empty() || credentials.is_empty() {
        return None;
    }
    // `scheme.lower() != 'bearer'`: only ASCII letters can lower-case to it.
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(credentials.to_owned())
}

/// `secrets.compare_digest(a, b)` on two `str`: constant time in the length of `b`; text
/// with a character outside ASCII raises `TypeError`, which nothing handles (a 500).
fn compare_digest(a: &str, b: &str) -> Result<bool, Internal> {
    if !a.is_ascii() || !b.is_ascii() {
        return Err(Internal(
            "TypeError: comparing strings with non-ASCII characters is not supported".to_owned(),
        ));
    }
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut differ = u8::from(a.len() != b.len());
    for (index, byte) in b.iter().enumerate() {
        differ |= byte ^ a.get(index).copied().unwrap_or(!byte);
    }
    Ok(differ == 0)
}

/// `make_mutation_auth(api_token)`: the check every mutation route runs before its body is
/// validated (after it is read and parsed, as FastAPI orders it).
#[derive(Clone, Default)]
pub struct MutationAuth {
    token: Option<Arc<str>>,
}

impl std::fmt::Debug for MutationAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutationAuth")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl MutationAuth {
    pub fn new(api_token: Option<&str>) -> Self {
        Self {
            token: api_token.map(Arc::from),
        }
    }

    /// Whether a token is configured (`auth_required` in the health and session answers).
    pub fn required(&self) -> bool {
        self.token.is_some()
    }

    /// `require_mutation_auth`: `Ok(None)` lets the request through, `Ok(Some(401))` refuses
    /// it, `Err` is the `TypeError` of a non-ASCII token or credential.
    pub fn check(&self, request: &Request) -> Result<Option<Response>, Internal> {
        let Some(token) = &self.token else {
            return Ok(None);
        };
        let Some(credentials) = bearer_credentials(request) else {
            return Ok(Some(unauthorized()));
        };
        if compare_digest(&credentials, token)? {
            Ok(None)
        } else {
            Ok(Some(unauthorized()))
        }
    }

    /// Run `check` and turn a refusal into the route's answer.
    pub fn refuse(&self, request: &Request) -> Option<Answer> {
        match self.check(request) {
            Ok(None) => None,
            Ok(Some(response)) => Some(Ok(response)),
            Err(error) => Some(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http::{HeaderMap, HeaderValue, Method};

    use super::{MutationAuth, bearer_credentials, compare_digest};
    use crate::http::Request;

    fn request(authorization: Option<&[u8]>) -> Request {
        let mut headers = HeaderMap::new();
        if let Some(value) = authorization {
            headers.insert("authorization", HeaderValue::from_bytes(value).expect("a header value"));
        }
        Request {
            method: Method::GET,
            path: "/api/auth/session".to_owned(),
            query: String::new(),
            headers,
            body: Body::empty(),
            params: Vec::new(),
            server: None,
        }
    }

    #[test]
    fn bearer_parsing_follows_fastapi() {
        let credentials = |value: &[u8]| bearer_credentials(&request(Some(value)));
        assert_eq!(credentials(b"Bearer abc"), Some("abc".to_owned()));
        assert_eq!(credentials(b"bEaReR   abc \t"), Some("abc".to_owned()));
        // Python's strip() also removes U+0085 and U+00A0, which latin-1 gives for these bytes.
        assert_eq!(credentials(b"Bearer \xa0abc\x85"), Some("abc".to_owned()));
        assert_eq!(credentials(b"Bearer abc def"), Some("abc def".to_owned()));
        assert_eq!(credentials(b"Bearer"), None);
        assert_eq!(credentials(b"Bearer    "), None);
        assert_eq!(credentials(b" Bearer abc"), None);
        assert_eq!(credentials(b"Basic abc"), None);
        assert_eq!(credentials(b"Bearerabc"), None);
        assert_eq!(bearer_credentials(&request(None)), None);
    }

    #[test]
    fn compare_digest_refuses_non_ascii_like_python() {
        assert!(compare_digest("token", "token").expect("ascii"));
        assert!(!compare_digest("token", "tokens").expect("ascii"));
        assert!(!compare_digest("", "token").expect("ascii"));
        assert!(!compare_digest("tokex", "token").expect("ascii"));
        assert!(compare_digest("tökén", "token").is_err());
        assert!(compare_digest("token", "tökén").is_err());
    }

    #[test]
    fn a_non_ascii_credential_is_a_server_error_and_no_token_lets_everyone_in() {
        let auth = MutationAuth::new(Some("secret"));
        assert!(auth.check(&request(Some(b"Bearer s\xe9cret"))).is_err());
        assert!(auth.check(&request(Some(b"Bearer secret"))).expect("ascii").is_none());
        assert!(auth.check(&request(None)).expect("no header").is_some());
        let open = MutationAuth::new(None);
        assert!(open.check(&request(Some(b"Bearer s\xe9cret"))).expect("open").is_none());
        assert!(!format!("{auth:?}").contains("secret"));
    }
}
