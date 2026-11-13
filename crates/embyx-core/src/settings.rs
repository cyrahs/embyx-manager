//! Deployment-level settings read from the environment.
//!
//! Python origin: `src/embyx_manager/settings.py`. Only what must be known before the
//! database is reachable, or what should not be changeable from the browser, lives here;
//! everything else is in the config store (cross-version contract 9: the same variables,
//! defaults and validation).

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::pytext::{py_int, py_repr, py_strip};

/// `EMBYX_MANAGER_DATABASE_URL`'s default.
pub const DEFAULT_DATABASE_URL: &str = "postgresql://localhost/embyx_manager";

/// Why the environment was refused: Python raises `ValueError` with this text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SettingsError(pub String);

impl SettingsError {
    /// The Python exception class.
    pub const CLASS: &'static str = "ValueError";
}

/// `Settings` (a frozen dataclass in Python). `Debug` leaves the database URL and the two
/// tokens out, since the URL may carry a password.
#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    pub database_url: String,
    pub api_token: Option<String>,
    pub tls_terminated: bool,
    pub host: String,
    pub port: u64,
    pub max_request_bytes: u64,
    /// Per-plan limits and submission concurrency stay here: raising them raises the
    /// request pressure on JavBus, Sukebei and CloudDrive during a fill-actor scan, which
    /// is an operator call rather than a UI toggle.
    pub max_actors: u64,
    pub max_videos: u64,
    pub magnet_concurrency: u64,
    /// A second listener for the home transcode worker's endpoints alone, which its
    /// WireGuard gateway forwards to; `None` leaves the worker out.
    pub transcode_port: Option<u64>,
    /// A bearer token the worker must also present, when set.
    pub transcode_token: Option<String>,
}

impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settings")
            .field("database_url", &"<redacted>")
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("tls_terminated", &self.tls_terminated)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_actors", &self.max_actors)
            .field("max_videos", &self.max_videos)
            .field("magnet_concurrency", &self.magnet_concurrency)
            .field("transcode_port", &self.transcode_port)
            .field("transcode_token", &self.transcode_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            database_url: DEFAULT_DATABASE_URL.to_owned(),
            api_token: None,
            tls_terminated: false,
            host: "127.0.0.1".to_owned(),
            port: 8000,
            max_request_bytes: 65_536,
            max_actors: 20,
            max_videos: 2_000,
            magnet_concurrency: 8,
            transcode_port: None,
            transcode_token: None,
        }
    }
}

/// An environment to read from: the process's own, or a table in tests.
pub trait Environment {
    fn get(&self, name: &str) -> Option<String>;
}

/// The process environment. A value that is not UTF-8 is read lossily (Python decodes it
/// with `surrogateescape`; none of these variables is ever anything but ASCII; DIVERGENCES.md D19).
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
    }
}

impl<F: Fn(&str) -> Option<String>> Environment for F {
    fn get(&self, name: &str) -> Option<String> {
        self(name)
    }
}

const OUT_OF_RANGE: &str = "integer out of range: ";

/// `_positive_int`: `int(raw)` when set, else the default; below 1 is refused.
fn positive_int(env: &impl Environment, name: &str, default: u64) -> Result<u64, SettingsError> {
    let not_positive = || SettingsError(format!("{name} must be positive"));
    let value = match env.get(name) {
        Some(raw) => match py_int(&raw) {
            Ok(value) => value,
            // Beyond `i128` but negative: Python reads it and refuses it as not positive.
            Err(message) if message.starts_with(OUT_OF_RANGE) && py_strip(&raw).starts_with('-') => {
                return Err(not_positive());
            }
            Err(message) => return Err(SettingsError(message)),
        },
        None => i128::from(default),
    };
    if value < 1 {
        return Err(not_positive());
    }
    u64::try_from(value).map_err(|_| SettingsError(format!("{OUT_OF_RANGE}{}", py_repr(&value.to_string()))))
}

/// `_boolean`: unset is false; `1 true yes on` / `0 false no off`, case-folded.
fn boolean(env: &impl Environment, name: &str) -> Result<bool, SettingsError> {
    let Some(raw) = env.get(name) else {
        return Ok(false);
    };
    let folded = casefold(&raw);
    if ["1", "true", "yes", "on"].contains(&folded.as_str()) {
        return Ok(true);
    }
    if ["0", "false", "no", "off"].contains(&folded.as_str()) {
        return Ok(false);
    }
    Err(SettingsError(format!("{name} must be a boolean")))
}

/// `str.casefold()` as far as comparing with lower-case ASCII words goes: lower-casing, plus
/// the characters whose case folding (not their lower case) is made of ASCII letters (the
/// whole list, from Python 3.13's tables).
fn casefold(text: &str) -> String {
    let mut folded = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\u{df}' | '\u{1e9e}' => folded.push_str("ss"),
            '\u{17f}' => folded.push('s'),
            '\u{212a}' => folded.push('k'),
            '\u{fb00}' => folded.push_str("ff"),
            '\u{fb01}' => folded.push_str("fi"),
            '\u{fb02}' => folded.push_str("fl"),
            '\u{fb03}' => folded.push_str("ffi"),
            '\u{fb04}' => folded.push_str("ffl"),
            '\u{fb05}' | '\u{fb06}' => folded.push_str("st"),
            other => folded.extend(other.to_lowercase()),
        }
    }
    folded
}

/// `_is_loopback_host`: `localhost` (case-folded) or `ipaddress.ip_address(host).is_loopback`.
fn is_loopback_host(host: &str) -> bool {
    if casefold(host) == "localhost" {
        return true;
    }
    if let Ok(address) = host.parse::<Ipv4Addr>() {
        return address.is_loopback();
    }
    // `ip_address` takes a non-empty `%scope` suffix on IPv6 and ignores it here.
    let address = match host.split_once('%') {
        Some((address, scope)) if !scope.is_empty() && !scope.contains('%') => address,
        Some(_) => return false,
        None => host,
    };
    // Python accepts only ASCII digits and hex letters (no brackets, no zone of its own).
    if !address.is_ascii() {
        return false;
    }
    address
        .parse::<Ipv6Addr>()
        .is_ok_and(|address| match address.to_ipv4_mapped() {
            Some(mapped) => mapped.is_loopback(),
            None => address.is_loopback(),
        })
}

impl Settings {
    /// `Settings.from_env()`.
    pub fn from_env() -> Result<Self, SettingsError> {
        Self::from_environment(&ProcessEnvironment)
    }

    /// `Settings.from_env()` over any environment, in Python's order of evaluation (the
    /// first refused variable is the one reported).
    pub fn from_environment(env: &impl Environment) -> Result<Self, SettingsError> {
        let non_empty = |name: &str| env.get(name).filter(|value| !value.is_empty());
        let database_url = env
            .get("EMBYX_MANAGER_DATABASE_URL")
            .unwrap_or_else(|| DEFAULT_DATABASE_URL.to_owned());
        let api_token = non_empty("EMBYX_MANAGER_API_TOKEN");
        let tls_terminated = boolean(env, "EMBYX_MANAGER_TLS_TERMINATED")?;
        let host = env.get("EMBYX_MANAGER_HOST").unwrap_or_else(|| "127.0.0.1".to_owned());
        let port = positive_int(env, "EMBYX_MANAGER_PORT", 8000)?;
        let max_request_bytes = positive_int(env, "EMBYX_MANAGER_MAX_REQUEST_BYTES", 65_536)?;
        let max_actors = positive_int(env, "EMBYX_MANAGER_MAX_ACTORS", 20)?;
        let max_videos = positive_int(env, "EMBYX_MANAGER_MAX_VIDEOS", 2_000)?;
        let magnet_concurrency = positive_int(env, "EMBYX_MANAGER_MAGNET_CONCURRENCY", 8)?;
        let transcode_port = if non_empty("EMBYX_MANAGER_TRANSCODE_PORT").is_some() {
            Some(positive_int(env, "EMBYX_MANAGER_TRANSCODE_PORT", 1)?)
        } else {
            None
        };
        let transcode_token = non_empty("EMBYX_MANAGER_TRANSCODE_TOKEN");
        let settings = Self {
            database_url,
            api_token,
            tls_terminated,
            host,
            port,
            max_request_bytes,
            max_actors,
            max_videos,
            magnet_concurrency,
            transcode_port,
            transcode_token,
        };
        settings.validate_exposure()?;
        Ok(settings)
    }

    /// `validate_exposure`: a non-loopback bind needs the API token and TLS termination.
    pub fn validate_exposure(&self) -> Result<(), SettingsError> {
        if !is_loopback_host(&self.host) {
            if self.api_token.is_none() {
                return Err(SettingsError(
                    "EMBYX_MANAGER_API_TOKEN is required when binding to a non-loopback host".to_owned(),
                ));
            }
            if !self.tls_terminated {
                return Err(SettingsError(
                    "EMBYX_MANAGER_TLS_TERMINATED must be true when binding to a non-loopback host".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{Settings, SettingsError};
    use crate::golden;
    use crate::pyjson::Value;

    fn from(pairs: &[(&str, &str)]) -> Result<Settings, SettingsError> {
        let env: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (format!("EMBYX_MANAGER_{name}"), (*value).to_owned()))
            .collect();
        Settings::from_environment(&|name: &str| env.get(name).cloned())
    }

    fn optional_string(value: &Value) -> Option<String> {
        match value {
            Value::Null => None,
            other => Some(golden::string(other)),
        }
    }

    fn optional_int(value: &Value) -> Option<u64> {
        match value {
            Value::Null => None,
            other => Some(u64::try_from(golden::int(other)).expect("a positive setting")),
        }
    }

    fn int(value: &Value) -> u64 {
        u64::try_from(golden::int(value)).expect("a positive setting")
    }

    #[test]
    fn environments_match_the_python_baseline() {
        let document = golden::load("settings/env.json");
        for case in golden::cases(&document) {
            let env: Vec<(String, String)> = golden::get(case, "env")
                .as_dict()
                .expect("env")
                .iter()
                .map(|(key, value)| (key.as_str().expect("string key").to_owned(), golden::string(value)))
                .collect();
            let pairs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let result = from(&pairs);
            if let Some(error) = case.get("error") {
                let error_text = result.expect_err(&format!("{env:?} is refused"));
                assert_eq!(golden::string(golden::get(error, "type")), SettingsError::CLASS);
                assert_eq!(error_text.0, golden::string(golden::get(error, "message")), "{env:?}");
                continue;
            }
            let expected = golden::get(case, "settings");
            // D13: Python takes integers of any size; the port refuses those beyond u64.
            if golden::int(golden::get(expected, "port")) > i128::from(u64::MAX) {
                let error = result.expect_err("beyond u64");
                assert!(error.0.starts_with("integer out of range: "), "{error}");
                continue;
            }
            let settings = result.unwrap_or_else(|error| panic!("{env:?}: {error}"));
            let want = Settings {
                database_url: golden::string(golden::get(expected, "database_url")),
                api_token: optional_string(golden::get(expected, "api_token")),
                tls_terminated: matches!(golden::get(expected, "tls_terminated"), Value::Bool(true)),
                host: golden::string(golden::get(expected, "host")),
                port: int(golden::get(expected, "port")),
                max_request_bytes: int(golden::get(expected, "max_request_bytes")),
                max_actors: int(golden::get(expected, "max_actors")),
                max_videos: int(golden::get(expected, "max_videos")),
                magnet_concurrency: int(golden::get(expected, "magnet_concurrency")),
                transcode_port: optional_int(golden::get(expected, "transcode_port")),
                transcode_token: optional_string(golden::get(expected, "transcode_token")),
            };
            assert_eq!(settings, want, "{env:?}");
        }
    }

    #[test]
    fn debug_leaves_secrets_out() {
        let settings = from(&[
            ("DATABASE_URL", "postgresql://user:hunter2@db/embyx"),
            ("API_TOKEN", "token-value"),
            ("TRANSCODE_TOKEN", "worker-value"),
        ])
        .expect("valid");
        let text = format!("{settings:?}");
        for secret in ["hunter2", "token-value", "worker-value"] {
            assert!(!text.contains(secret), "{text}");
        }
    }

    // py: tests/test_settings.py::test_settings_read_the_deployment_level_environment
    #[test]
    fn settings_read_the_deployment_level_environment() {
        let settings = from(&[
            ("DATABASE_URL", "postgresql://db.internal/embyx"),
            ("MAX_ACTORS", "5"),
            ("MAX_VIDEOS", "50"),
            ("MAGNET_CONCURRENCY", "2"),
        ])
        .expect("valid");
        assert_eq!(settings.database_url, "postgresql://db.internal/embyx");
        assert_eq!(settings.max_actors, 5);
        assert_eq!(settings.max_videos, 50);
        assert_eq!(settings.magnet_concurrency, 2);
    }

    // py: tests/test_settings.py::test_library_paths_are_not_read_from_the_environment
    #[test]
    fn library_paths_are_not_read_from_the_environment() {
        // The struct has no library-path fields at all; leftover variables change nothing.
        let settings = from(&[
            ("ACTOR_ROOT", "/media/actor"),
            ("APPLY_ENABLED", "true"),
            ("CLOUD_SOURCE_ROOTS", "/cloud/a"),
        ])
        .expect("valid");
        assert_eq!(settings, Settings::default());
    }

    // py: tests/test_settings.py::test_positive_limits_are_rejected_when_zero
    #[test]
    fn positive_limits_are_rejected_when_zero() {
        let error = from(&[("MAX_ACTORS", "0")]).expect_err("refused");
        assert!(error.0.contains("MAX_ACTORS must be positive"), "{error}");
    }

    // py: tests/test_settings.py::test_non_loopback_binding_requires_token
    #[test]
    fn non_loopback_binding_requires_token() {
        let error = from(&[("HOST", "192.0.2.1")]).expect_err("refused");
        assert!(error.0.contains("API_TOKEN"), "{error}");
    }

    // py: tests/test_settings.py::test_non_loopback_binding_accepts_token
    #[test]
    fn non_loopback_binding_accepts_token() {
        let settings = from(&[
            ("HOST", "192.0.2.1"),
            ("API_TOKEN", "configured-at-runtime"),
            ("TLS_TERMINATED", "true"),
        ])
        .expect("valid");
        assert_eq!(settings.host, "192.0.2.1");
    }

    // py: tests/test_settings.py::test_non_loopback_binding_requires_tls_termination
    #[test]
    fn non_loopback_binding_requires_tls_termination() {
        let error = from(&[("HOST", "192.0.2.1"), ("API_TOKEN", "configured-at-runtime")]).expect_err("refused");
        assert!(error.0.contains("TLS_TERMINATED"), "{error}");
    }
}
