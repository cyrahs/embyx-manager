//! The HTTP error envelope every feature router shares.
//!
//! Python origin: `src/embyx_manager/errors.py`. Renders as `{"error": {"code": ...}}` with
//! an explicit status code (the axum rendering is WP4's).

/// `ApiError(status_code, code)`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code}")]
pub struct ApiError {
    pub status_code: u16,
    pub code: String,
}

impl ApiError {
    pub fn new(status_code: u16, code: impl Into<String>) -> Self {
        Self {
            status_code,
            code: code.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ApiError;

    #[test]
    fn carries_the_status_and_renders_as_its_code() {
        let error = ApiError::new(404, "unknown_config_section");
        assert_eq!(error.status_code, 404);
        assert_eq!(error.code, "unknown_config_section");
        assert_eq!(error.to_string(), "unknown_config_section");
    }
}
