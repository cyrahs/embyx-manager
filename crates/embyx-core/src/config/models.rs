//! The nine configuration sections stored in `app_config` and edited from the web UI.
//!
//! Python origin: `src/embyx_manager/config/models.py`. Each section validates exactly as
//! its pydantic model does (lax coercion, field validators in declaration order, then the
//! model validator, unknown keys forbidden) and dumps the same fields in the same order, so
//! the stored bytes and the key set never differ between the builds (cross-version contract
//! 2). Secret fields are listed per section; the API masks them on read.

use std::fmt;

use super::lax::{ErrorDetail, Mode, Recorded, ValidationError, Validator, lax_bool, lax_float, lax_int, lax_str};
use super::normalize::{
    normalize_absolute_path, normalize_absolute_paths, normalize_http_base_url, normalize_relative_subpath, repr,
};
use crate::cron::CronSpec;
use crate::pyjson::{Dict, Value};
use crate::pytext::{py_split, py_strip};

/// What every section offers the store and the API.
pub trait Section: Sized + Clone + PartialEq + Default + fmt::Debug + Send + Sync + 'static {
    /// The `app_config.section` key (`clouddrive`).
    const NAME: &'static str;
    /// The pydantic model's class name (`CloudDriveConfig`), the title of its errors.
    const MODEL: &'static str;
    /// Field names in declaration order, which is also the stored order.
    const FIELDS: &'static [&'static str];
    /// `SECRET_FIELDS`, sorted.
    const SECRET_FIELDS: &'static [&'static str] = &[];

    /// The fields, converted and checked; `Err` holds every error found.
    fn validate_fields(validator: &mut Validator<'_>) -> Option<Self>;

    /// The model validator (`mode='after'`), run once every field passed.
    fn validate_model(self) -> Result<Self, String> {
        Ok(self)
    }

    /// `model_dump()`: every field in order (tuples as lists, nested models as objects).
    fn dump(&self) -> Value;

    /// The value of a secret field, for [`secret_flags`].
    #[allow(clippy::unnecessary_literal_bound)] // the sections with secrets return a field
    fn secret(&self, _field: &str) -> &str {
        ""
    }

    /// `model_validate(value)` (Python mode) or the second half of `model_validate_json`.
    fn validate(value: &Value, mode: Mode) -> Result<Self, ValidationError> {
        validate_model::<Self>(value, mode, Vec::new()).map_err(|errors| ValidationError {
            title: Self::MODEL,
            errors,
        })
    }

    /// `model_validate_json(text)`.
    fn validate_json(text: &str) -> Result<Self, ValidationError> {
        let value = super::lax::parse_json(text).map_err(|error| ValidationError {
            title: Self::MODEL,
            errors: vec![error],
        })?;
        Self::validate(&value, Mode::Json)
    }

    /// `model_dump_json()`: the stored bytes.
    fn dump_json(&self) -> String {
        super::dump::pydantic_json(&self.dump())
    }
}

/// Validate a model (top level or nested at `prefix`): fields, unknown keys, then the model
/// validator, whose refusal is a `value_error` at the model's own location.
pub(crate) fn validate_model<S: Section>(value: &Value, mode: Mode, prefix: Vec<Value>) -> Result<S, Vec<ErrorDetail>> {
    let mut validator =
        Validator::new(value, mode, S::MODEL, S::FIELDS, prefix.clone()).map_err(|error| vec![error])?;
    let section = S::validate_fields(&mut validator);
    validator.finish()?;
    let section = section.expect("every field converted when no error was recorded");
    section.validate_model().map_err(|message| {
        vec![ErrorDetail {
            kind: "value_error",
            loc: prefix,
            msg: format!("Value error, {message}"),
        }]
    })
}

/// `masked_values(section)`: the stored JSON read back (`json.loads(model_dump_json())`),
/// secrets blanked.
pub fn masked_values<S: Section>(section: &S) -> Value {
    let mut values = crate::pyjson::loads(&section.dump_json()).expect("a dumped section is JSON");
    if let Value::Dict(dict) = &mut values {
        for field in S::SECRET_FIELDS {
            dict.insert(*field, Value::Str(String::new()));
        }
    }
    values
}

/// `secret_flags(section)`: per secret field (sorted), whether it is set.
pub fn secret_flags<S: Section>(section: &S) -> Vec<(&'static str, bool)> {
    S::SECRET_FIELDS
        .iter()
        .map(|field| (*field, !section.secret(field).is_empty()))
        .collect()
}

// ---------- field helpers ----------

fn at(name: &str) -> Vec<Value> {
    vec![Value::Str(name.to_owned())]
}

fn string(v: &mut Validator<'_>, name: &str, value: &Value) -> Result<String, Recorded> {
    v.scalar_at(&at(name), value, lax_str)
}

fn boolean(v: &mut Validator<'_>, name: &str, value: &Value) -> Result<bool, Recorded> {
    v.scalar_at(&at(name), value, lax_bool)
}

fn integer(v: &mut Validator<'_>, name: &str, value: &Value) -> Result<i64, Recorded> {
    v.scalar_at(&at(name), value, lax_int)
}

fn float(v: &mut Validator<'_>, name: &str, value: &Value) -> Result<f64, Recorded> {
    v.scalar_at(&at(name), value, lax_float)
}

fn strings(v: &mut Validator<'_>, loc: &[Value], value: &Value) -> Result<Vec<String>, Recorded> {
    v.tuple(loc, value, |v, loc, item| v.scalar_at(&loc, item, lax_str))
}

fn string_map(v: &mut Validator<'_>, name: &str, value: &Value) -> Result<Vec<(String, String)>, Recorded> {
    v.dict(&at(name), value, |v, loc, item| v.scalar_at(&loc, item, lax_str))
}

fn positive(value: i64) -> Result<i64, String> {
    if value < 1 {
        Err("must be positive".to_owned())
    } else {
        Ok(value)
    }
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

fn list_of(values: &[String]) -> Value {
    Value::List(values.iter().cloned().map(Value::Str).collect())
}

fn map_of(entries: &[(String, String)]) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key.clone(), Value::Str(value.clone()));
    }
    Value::Dict(dict)
}

// ---------- clouddrive ----------

/// `CloudDriveConfig`.
#[derive(Clone, PartialEq, Eq)]
pub struct CloudDriveConfig {
    pub address: String,
    pub api_token: String,
    pub secure: bool,
}

impl Default for CloudDriveConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            api_token: String::new(),
            secure: true,
        }
    }
}

impl fmt::Debug for CloudDriveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudDriveConfig")
            .field("address", &self.address)
            .field("api_token", &if self.api_token.is_empty() { "" } else { "<redacted>" })
            .field("secure", &self.secure)
            .finish()
    }
}

impl CloudDriveConfig {
    pub fn configured(&self) -> bool {
        !self.address.is_empty() && !self.api_token.is_empty()
    }
}

impl Section for CloudDriveConfig {
    const NAME: &'static str = "clouddrive";
    const MODEL: &'static str = "CloudDriveConfig";
    const FIELDS: &'static [&'static str] = &["address", "api_token", "secure"];
    const SECRET_FIELDS: &'static [&'static str] = &["api_token"];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let address = v.field("address", String::new, |v, x| string(v, "address", x), Ok);
        let api_token = v.field("api_token", String::new, |v, x| string(v, "api_token", x), Ok);
        let secure = v.field("secure", || true, |v, x| boolean(v, "secure", x), Ok);
        Some(Self {
            address: address?,
            api_token: api_token?,
            secure: secure?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("address", Value::Str(self.address.clone())),
            ("api_token", Value::Str(self.api_token.clone())),
            ("secure", Value::Bool(self.secure)),
        ])
    }

    fn secret(&self, field: &str) -> &str {
        if field == "api_token" { &self.api_token } else { "" }
    }
}

// ---------- emby ----------

/// `EmbyConfig`: the Emby server the playlist sync maintains lists on.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct EmbyConfig {
    /// HTTP base URL; inside the cluster the instance's Service.
    pub address: String,
    /// An API key generated on the server (Settings, Advanced, API Keys).
    pub api_key: String,
}

impl fmt::Debug for EmbyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbyConfig")
            .field("address", &self.address)
            .field("api_key", &if self.api_key.is_empty() { "" } else { "<redacted>" })
            .finish()
    }
}

impl EmbyConfig {
    pub fn configured(&self) -> bool {
        !self.address.is_empty() && !self.api_key.is_empty()
    }
}

impl Section for EmbyConfig {
    const NAME: &'static str = "emby";
    const MODEL: &'static str = "EmbyConfig";
    const FIELDS: &'static [&'static str] = &["address", "api_key"];
    const SECRET_FIELDS: &'static [&'static str] = &["api_key"];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let address = v.field(
            "address",
            String::new,
            |v, x| string(v, "address", x),
            |value| normalize_http_base_url("emby.address", &value),
        );
        let api_key = v.field("api_key", String::new, |v, x| string(v, "api_key", x), Ok);
        Some(Self {
            address: address?,
            api_key: api_key?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("address", Value::Str(self.address.clone())),
            ("api_key", Value::Str(self.api_key.clone())),
        ])
    }

    fn secret(&self, field: &str) -> &str {
        if field == "api_key" { &self.api_key } else { "" }
    }
}

// ---------- avid ----------

/// `AvidRulesConfig`: video-ID parsing rules shared by the pipelines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AvidRulesConfig {
    pub id_exceptions: Vec<String>,
    pub ignored_id_patterns: Vec<String>,
}

impl Section for AvidRulesConfig {
    const NAME: &'static str = "avid";
    const MODEL: &'static str = "AvidRulesConfig";
    const FIELDS: &'static [&'static str] = &["id_exceptions", "ignored_id_patterns"];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let id_exceptions = v.field(
            "id_exceptions",
            Vec::new,
            |v, x| strings(v, &at("id_exceptions"), x),
            Ok,
        );
        let ignored_id_patterns = v.field(
            "ignored_id_patterns",
            Vec::new,
            |v, x| strings(v, &at("ignored_id_patterns"), x),
            Ok,
        );
        Some(Self {
            id_exceptions: id_exceptions?,
            ignored_id_patterns: ignored_id_patterns?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("id_exceptions", list_of(&self.id_exceptions)),
            ("ignored_id_patterns", list_of(&self.ignored_id_patterns)),
        ])
    }
}

// ---------- rss ----------

/// `RssCategory`: one subscription category and the offline directory its items download to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RssCategory {
    /// The category (label) whose subscriptions this entry ingests.
    pub label: String,
    /// The CloudDrive API path this category's offline tasks are queued under.
    pub task_dir_path: String,
}

impl Section for RssCategory {
    const NAME: &'static str = "rss.categories";
    const MODEL: &'static str = "RssCategory";
    const FIELDS: &'static [&'static str] = &["label", "task_dir_path"];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let label = v.required(
            "label",
            |v, x| string(v, "label", x),
            |value| {
                let trimmed = py_strip(&value);
                if trimmed.is_empty() {
                    Err("rss.categories label must not be empty".to_owned())
                } else {
                    Ok(trimmed.to_owned())
                }
            },
        );
        let task_dir_path = v.required(
            "task_dir_path",
            |v, x| string(v, "task_dir_path", x),
            |value| {
                let normalized = normalize_absolute_path("rss.categories task_dir_path", &value)?;
                if normalized.is_empty() {
                    Err("rss.categories task_dir_path must not be empty".to_owned())
                } else {
                    Ok(normalized)
                }
            },
        );
        Some(Self {
            label: label?,
            task_dir_path: task_dir_path?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("label", Value::Str(self.label.clone())),
            ("task_dir_path", Value::Str(self.task_dir_path.clone())),
        ])
    }
}

/// `RssConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RssConfig {
    pub enabled: bool,
    pub interval_seconds: i64,
    /// Every configured category is ingested on each scheduled run, in this order, which is
    /// also their precedence. Empty until an operator adds one: a category needs a directory.
    pub categories: Vec<RssCategory>,
    /// Resolve-failure cooldown for every acquisition intake source, not just RSS.
    pub failed_avid_cooldown_seconds: i64,
}

impl Default for RssConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 1800,
            categories: Vec::new(),
            failed_avid_cooldown_seconds: 86_400,
        }
    }
}

fn unique_labels(categories: Vec<RssCategory>) -> Result<Vec<RssCategory>, String> {
    // A repeated label would ingest the same items twice in one run, and the second
    // entry's directory would silently lose to the first.
    let mut duplicates: Vec<&str> = categories
        .iter()
        .map(|category| category.label.as_str())
        .filter(|label| categories.iter().filter(|other| other.label == *label).count() > 1)
        .collect();
    duplicates.sort_unstable();
    duplicates.dedup();
    if duplicates.is_empty() {
        return Ok(categories);
    }
    let listed: Vec<String> = duplicates.iter().map(|label| repr(label)).collect();
    Err(format!(
        "rss.categories must not repeat the label {}",
        listed.join(", ")
    ))
}

impl Section for RssConfig {
    const NAME: &'static str = "rss";
    const MODEL: &'static str = "RssConfig";
    const FIELDS: &'static [&'static str] = &[
        "enabled",
        "interval_seconds",
        "categories",
        "failed_avid_cooldown_seconds",
    ];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let enabled = v.field("enabled", || false, |v, x| boolean(v, "enabled", x), Ok);
        let interval_seconds = v.field(
            "interval_seconds",
            || 1800,
            |v, x| integer(v, "interval_seconds", x),
            positive,
        );
        let categories = v.field(
            "categories",
            Vec::new,
            |v, x| {
                v.tuple(&at("categories"), x, |v, loc, item| {
                    let prefix = v.nested_prefix(&loc);
                    super::models::validate_model::<RssCategory>(item, v.mode(), prefix).map_err(|errors| {
                        for error in errors {
                            v.push_detail(error);
                        }
                        Recorded
                    })
                })
            },
            unique_labels,
        );
        let failed_avid_cooldown_seconds = v.field(
            "failed_avid_cooldown_seconds",
            || 86_400,
            |v, x| integer(v, "failed_avid_cooldown_seconds", x),
            positive,
        );
        Some(Self {
            enabled: enabled?,
            interval_seconds: interval_seconds?,
            categories: categories?,
            failed_avid_cooldown_seconds: failed_avid_cooldown_seconds?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("enabled", Value::Bool(self.enabled)),
            ("interval_seconds", Value::from(self.interval_seconds)),
            (
                "categories",
                Value::List(self.categories.iter().map(Section::dump).collect()),
            ),
            (
                "failed_avid_cooldown_seconds",
                Value::from(self.failed_avid_cooldown_seconds),
            ),
        ])
    }
}

// ---------- archive ----------

/// `ArchiveConfig`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveConfig {
    pub enabled: bool,
    /// When the fallback scan runs: a five-field cron expression in the server's local time.
    pub scan_cron: String,
    pub src_dir: String,
    pub dst_dir: String,
    /// Per-subdirectory routing below `src_dir`/`dst_dir`, in insertion order.
    pub mapping: Vec<(String, String)>,
    /// Same shape as `mapping`; these routes run first.
    pub priority_mapping: Vec<(String, String)>,
    pub min_size_mb: i64,
    /// Destination subdirectory -> brands routed into it.
    pub brand_mapping: Vec<(String, Vec<String>)>,
    pub tracker_interval_seconds: i64,
    pub stall_timeout_hours: i64,
    pub max_attempts: i64,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            scan_cron: "0 4 * * *".to_owned(),
            src_dir: String::new(),
            dst_dir: String::new(),
            mapping: Vec::new(),
            priority_mapping: Vec::new(),
            min_size_mb: 0,
            brand_mapping: Vec::new(),
            tracker_interval_seconds: 1800,
            stall_timeout_hours: 24,
            max_attempts: 5,
        }
    }
}

fn validate_scan_cron(value: &str) -> Result<String, String> {
    let expr = py_split(value).collect::<Vec<_>>().join(" ");
    CronSpec::parse(&expr).map_err(|error| format!("archive.scan_cron is not a valid cron expression: {error}"))?;
    Ok(expr)
}

fn validate_routes(label: &str, value: Vec<(String, String)>) -> Result<Vec<(String, String)>, String> {
    let mut mapping: Vec<(String, String)> = Vec::with_capacity(value.len());
    for (src, dst) in value {
        let source = normalize_relative_subpath(&format!("{label} source"), &src)?;
        if mapping.iter().any(|(existing, _)| *existing == source) {
            return Err(format!(
                "{label} must not repeat the source subdirectory {}",
                repr(&source)
            ));
        }
        let destination = normalize_relative_subpath(&format!("{label} destination"), &dst)?;
        mapping.push((source, destination));
    }
    Ok(mapping)
}

fn validate_brand_mapping(value: Vec<(String, Vec<String>)>) -> Result<Vec<(String, Vec<String>)>, String> {
    let mut mapping: Vec<(String, Vec<String>)> = Vec::with_capacity(value.len());
    for (dst, brands) in value {
        let destination = normalize_relative_subpath("archive.brand_mapping destination", &dst)?;
        if mapping.iter().any(|(existing, _)| *existing == destination) {
            return Err(format!(
                "archive.brand_mapping must not repeat the destination subdirectory {}",
                repr(&destination)
            ));
        }
        // Brands are matched against upper-cased video IDs, so a lower-cased entry here
        // would silently never route anything.
        let mut normalized: Vec<String> = Vec::new();
        for brand in &brands {
            let stripped = py_strip(brand);
            if stripped.is_empty() {
                continue;
            }
            let upper = stripped.to_uppercase();
            if !normalized.contains(&upper) {
                normalized.push(upper);
            }
        }
        if normalized.is_empty() {
            return Err(format!(
                "archive.brand_mapping[{}] must list at least one brand",
                repr(&destination)
            ));
        }
        mapping.push((destination, normalized));
    }
    Ok(mapping)
}

impl ArchiveConfig {
    pub fn configured(&self) -> bool {
        !self.src_dir.is_empty()
            && !self.dst_dir.is_empty()
            && (!self.mapping.is_empty() || !self.priority_mapping.is_empty())
    }
}

impl Section for ArchiveConfig {
    const NAME: &'static str = "archive";
    const MODEL: &'static str = "ArchiveConfig";
    const FIELDS: &'static [&'static str] = &[
        "enabled",
        "scan_cron",
        "src_dir",
        "dst_dir",
        "mapping",
        "priority_mapping",
        "min_size_mb",
        "brand_mapping",
        "tracker_interval_seconds",
        "stall_timeout_hours",
        "max_attempts",
    ];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let enabled = v.field("enabled", || false, |v, x| boolean(v, "enabled", x), Ok);
        let scan_cron = v.field(
            "scan_cron",
            || "0 4 * * *".to_owned(),
            |v, x| string(v, "scan_cron", x),
            |value| validate_scan_cron(&value),
        );
        let src_dir = v.field("src_dir", String::new, |v, x| string(v, "src_dir", x), Ok);
        let dst_dir = v.field("dst_dir", String::new, |v, x| string(v, "dst_dir", x), Ok);
        let mapping = v.field(
            "mapping",
            Vec::new,
            |v, x| string_map(v, "mapping", x),
            |value| validate_routes("archive.mapping", value),
        );
        let priority_mapping = v.field(
            "priority_mapping",
            Vec::new,
            |v, x| string_map(v, "priority_mapping", x),
            |value| validate_routes("archive.priority_mapping", value),
        );
        let min_size_mb = v.field(
            "min_size_mb",
            || 0,
            |v, x| integer(v, "min_size_mb", x),
            |value| {
                if value < 0 {
                    Err("must not be negative".to_owned())
                } else {
                    Ok(value)
                }
            },
        );
        let brand_mapping = v.field(
            "brand_mapping",
            Vec::new,
            |v, x| v.dict(&at("brand_mapping"), x, |v, loc, item| strings(v, &loc, item)),
            validate_brand_mapping,
        );
        let tracker_interval_seconds = v.field(
            "tracker_interval_seconds",
            || 1800,
            |v, x| integer(v, "tracker_interval_seconds", x),
            positive,
        );
        let stall_timeout_hours = v.field(
            "stall_timeout_hours",
            || 24,
            |v, x| integer(v, "stall_timeout_hours", x),
            positive,
        );
        let max_attempts = v.field("max_attempts", || 5, |v, x| integer(v, "max_attempts", x), positive);
        Some(Self {
            enabled: enabled?,
            scan_cron: scan_cron?,
            src_dir: src_dir?,
            dst_dir: dst_dir?,
            mapping: mapping?,
            priority_mapping: priority_mapping?,
            min_size_mb: min_size_mb?,
            brand_mapping: brand_mapping?,
            tracker_interval_seconds: tracker_interval_seconds?,
            stall_timeout_hours: stall_timeout_hours?,
            max_attempts: max_attempts?,
        })
    }

    fn validate_model(self) -> Result<Self, String> {
        // A destination may be shared by both tables, but a source subdirectory belongs to
        // exactly one category or the routing would be ambiguous.
        let mut overlap: Vec<&str> = self
            .mapping
            .iter()
            .map(|(source, _)| source.as_str())
            .filter(|source| self.priority_mapping.iter().any(|(other, _)| other == source))
            .collect();
        overlap.sort_unstable();
        overlap.dedup();
        if !overlap.is_empty() {
            let listed: Vec<String> = overlap.iter().map(|source| repr(source)).collect();
            return Err(format!(
                "archive.mapping and archive.priority_mapping must not share the source subdirectory {}",
                listed.join(", ")
            ));
        }
        Ok(self)
    }

    fn dump(&self) -> Value {
        let mut brand_mapping = Dict::new();
        for (destination, brands) in &self.brand_mapping {
            brand_mapping.insert(destination.clone(), list_of(brands));
        }
        object(vec![
            ("enabled", Value::Bool(self.enabled)),
            ("scan_cron", Value::Str(self.scan_cron.clone())),
            ("src_dir", Value::Str(self.src_dir.clone())),
            ("dst_dir", Value::Str(self.dst_dir.clone())),
            ("mapping", map_of(&self.mapping)),
            ("priority_mapping", map_of(&self.priority_mapping)),
            ("min_size_mb", Value::from(self.min_size_mb)),
            ("brand_mapping", Value::Dict(brand_mapping)),
            ("tracker_interval_seconds", Value::from(self.tracker_interval_seconds)),
            ("stall_timeout_hours", Value::from(self.stall_timeout_hours)),
            ("max_attempts", Value::from(self.max_attempts)),
        ])
    }
}

// ---------- mapping ----------

/// `MappingConfig`.
#[derive(Debug, Clone, PartialEq)]
pub struct MappingConfig {
    pub enabled: bool,
    pub src_dir: String,
    pub dst_dir: String,
    pub debounce_seconds: f64,
    pub full_sync_interval_seconds: i64,
}

impl Default for MappingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            src_dir: String::new(),
            dst_dir: String::new(),
            debounce_seconds: 2.0,
            full_sync_interval_seconds: 86_400,
        }
    }
}

impl MappingConfig {
    pub fn configured(&self) -> bool {
        !self.src_dir.is_empty() && !self.dst_dir.is_empty()
    }
}

impl Section for MappingConfig {
    const NAME: &'static str = "mapping";
    const MODEL: &'static str = "MappingConfig";
    const FIELDS: &'static [&'static str] = &[
        "enabled",
        "src_dir",
        "dst_dir",
        "debounce_seconds",
        "full_sync_interval_seconds",
    ];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let enabled = v.field("enabled", || false, |v, x| boolean(v, "enabled", x), Ok);
        let src_dir = v.field("src_dir", String::new, |v, x| string(v, "src_dir", x), Ok);
        let dst_dir = v.field("dst_dir", String::new, |v, x| string(v, "dst_dir", x), Ok);
        let debounce_seconds = v.field("debounce_seconds", || 2.0, |v, x| float(v, "debounce_seconds", x), Ok);
        let full_sync_interval_seconds = v.field(
            "full_sync_interval_seconds",
            || 86_400,
            |v, x| integer(v, "full_sync_interval_seconds", x),
            positive,
        );
        Some(Self {
            enabled: enabled?,
            src_dir: src_dir?,
            dst_dir: dst_dir?,
            debounce_seconds: debounce_seconds?,
            full_sync_interval_seconds: full_sync_interval_seconds?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("enabled", Value::Bool(self.enabled)),
            ("src_dir", Value::Str(self.src_dir.clone())),
            ("dst_dir", Value::Str(self.dst_dir.clone())),
            ("debounce_seconds", Value::Float(self.debounce_seconds)),
            (
                "full_sync_interval_seconds",
                Value::from(self.full_sync_interval_seconds),
            ),
        ])
    }
}

// ---------- playlists ----------

/// `PlaylistsConfig`: the ranking-to-playlist sync's schedule, source and fill directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaylistsConfig {
    pub enabled: bool,
    pub interval_seconds: i64,
    /// The page whose script names the current ranking database.
    pub source_url: String,
    /// The CloudDrive API directory a list's missing titles are queued under; empty means
    /// the RSS category labelled "Rank".
    pub task_dir_path: String,
}

impl Default for PlaylistsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 86_400,
            source_url: "https://jinjier.art/sql".to_owned(),
            task_dir_path: String::new(),
        }
    }
}

impl Section for PlaylistsConfig {
    const NAME: &'static str = "playlists";
    const MODEL: &'static str = "PlaylistsConfig";
    const FIELDS: &'static [&'static str] = &["enabled", "interval_seconds", "source_url", "task_dir_path"];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let enabled = v.field("enabled", || false, |v, x| boolean(v, "enabled", x), Ok);
        let interval_seconds = v.field(
            "interval_seconds",
            || 86_400,
            |v, x| integer(v, "interval_seconds", x),
            |value| {
                if value <= 0 {
                    Err("playlists.interval_seconds must be positive".to_owned())
                } else {
                    Ok(value)
                }
            },
        );
        let source_url = v.field(
            "source_url",
            || "https://jinjier.art/sql".to_owned(),
            |v, x| string(v, "source_url", x),
            |value| {
                let normalized = normalize_http_base_url("playlists.source_url", &value)?;
                if normalized.is_empty() {
                    Err("playlists.source_url must not be empty".to_owned())
                } else {
                    Ok(normalized)
                }
            },
        );
        let task_dir_path = v.field(
            "task_dir_path",
            String::new,
            |v, x| string(v, "task_dir_path", x),
            |value| normalize_absolute_path("playlists.task_dir_path", &value),
        );
        Some(Self {
            enabled: enabled?,
            interval_seconds: interval_seconds?,
            source_url: source_url?,
            task_dir_path: task_dir_path?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("enabled", Value::Bool(self.enabled)),
            ("interval_seconds", Value::from(self.interval_seconds)),
            ("source_url", Value::Str(self.source_url.clone())),
            ("task_dir_path", Value::Str(self.task_dir_path.clone())),
        ])
    }
}

// ---------- fill_actor ----------

/// `FillActorConfig`: library roots and move settings for the Fill Actor feature. Every
/// path defaults to empty; a deployment's real paths live in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillActorConfig {
    pub actor_root: String,
    pub additional_roots: Vec<String>,
    pub move_in_root: String,
    pub move_in_by_brand: bool,
    pub root_sentinel: String,
    pub apply_enabled: bool,
    pub cloud_strm_mount_prefix: String,
    pub cloud_source_roots: Vec<String>,
    pub cloud_move_in_root: String,
    /// The CloudDrive API path fill-actor submissions are queued under; empty means scans
    /// cannot submit.
    pub task_dir_path: String,
}

impl Default for FillActorConfig {
    fn default() -> Self {
        Self {
            actor_root: String::new(),
            additional_roots: Vec::new(),
            move_in_root: String::new(),
            move_in_by_brand: false,
            root_sentinel: ".embyx-root".to_owned(),
            apply_enabled: false,
            cloud_strm_mount_prefix: String::new(),
            cloud_source_roots: Vec::new(),
            cloud_move_in_root: String::new(),
            task_dir_path: String::new(),
        }
    }
}

impl FillActorConfig {
    pub fn cloud_configured(&self) -> bool {
        !self.cloud_strm_mount_prefix.is_empty()
            && !self.cloud_source_roots.is_empty()
            && !self.cloud_move_in_root.is_empty()
    }

    /// True once scanning has somewhere to look and somewhere to move into.
    pub fn configured(&self) -> bool {
        !self.actor_root.is_empty() && !self.additional_roots.is_empty() && !self.move_in_root.is_empty()
    }
}

fn absolute(name: &'static str) -> impl Fn(String) -> Result<String, String> {
    move |value| normalize_absolute_path(name, &value)
}

impl Section for FillActorConfig {
    const NAME: &'static str = "fill_actor";
    const MODEL: &'static str = "FillActorConfig";
    const FIELDS: &'static [&'static str] = &[
        "actor_root",
        "additional_roots",
        "move_in_root",
        "move_in_by_brand",
        "root_sentinel",
        "apply_enabled",
        "cloud_strm_mount_prefix",
        "cloud_source_roots",
        "cloud_move_in_root",
        "task_dir_path",
    ];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let actor_root = v.field(
            "actor_root",
            String::new,
            |v, x| string(v, "actor_root", x),
            absolute("fill_actor.actor_root"),
        );
        let additional_roots = v.field(
            "additional_roots",
            Vec::new,
            |v, x| strings(v, &at("additional_roots"), x),
            |value| normalize_absolute_paths("fill_actor.additional_roots", &value),
        );
        let move_in_root = v.field(
            "move_in_root",
            String::new,
            |v, x| string(v, "move_in_root", x),
            absolute("fill_actor.move_in_root"),
        );
        let move_in_by_brand = v.field(
            "move_in_by_brand",
            || false,
            |v, x| boolean(v, "move_in_by_brand", x),
            Ok,
        );
        let root_sentinel = v.field(
            "root_sentinel",
            || ".embyx-root".to_owned(),
            |v, x| string(v, "root_sentinel", x),
            |value| {
                let trimmed = py_strip(&value);
                if ["", ".", ".."].contains(&trimmed) || trimmed.contains('/') || trimmed.contains('\0') {
                    Err("fill_actor.root_sentinel must be a single path segment".to_owned())
                } else {
                    Ok(trimmed.to_owned())
                }
            },
        );
        let apply_enabled = v.field("apply_enabled", || false, |v, x| boolean(v, "apply_enabled", x), Ok);
        let cloud_strm_mount_prefix = v.field(
            "cloud_strm_mount_prefix",
            String::new,
            |v, x| string(v, "cloud_strm_mount_prefix", x),
            absolute("fill_actor.cloud_strm_mount_prefix"),
        );
        let cloud_source_roots = v.field(
            "cloud_source_roots",
            Vec::new,
            |v, x| strings(v, &at("cloud_source_roots"), x),
            |value| normalize_absolute_paths("fill_actor.cloud_source_roots", &value),
        );
        let cloud_move_in_root = v.field(
            "cloud_move_in_root",
            String::new,
            |v, x| string(v, "cloud_move_in_root", x),
            absolute("fill_actor.cloud_move_in_root"),
        );
        let task_dir_path = v.field(
            "task_dir_path",
            String::new,
            |v, x| string(v, "task_dir_path", x),
            absolute("fill_actor.task_dir_path"),
        );
        Some(Self {
            actor_root: actor_root?,
            additional_roots: additional_roots?,
            move_in_root: move_in_root?,
            move_in_by_brand: move_in_by_brand?,
            root_sentinel: root_sentinel?,
            apply_enabled: apply_enabled?,
            cloud_strm_mount_prefix: cloud_strm_mount_prefix?,
            cloud_source_roots: cloud_source_roots?,
            cloud_move_in_root: cloud_move_in_root?,
            task_dir_path: task_dir_path?,
        })
    }

    fn validate_model(self) -> Result<Self, String> {
        if !self.actor_root.is_empty() && self.additional_roots.contains(&self.actor_root) {
            return Err("fill_actor.actor_root must not also be an additional root".to_owned());
        }
        let cloud_values = [
            !self.cloud_strm_mount_prefix.is_empty(),
            !self.cloud_source_roots.is_empty(),
            !self.cloud_move_in_root.is_empty(),
        ];
        let all = cloud_values.iter().all(|&set| set);
        if cloud_values.iter().any(|&set| set) && !all {
            return Err(concat!(
                "fill_actor.cloud_strm_mount_prefix, fill_actor.cloud_source_roots, and ",
                "fill_actor.cloud_move_in_root must be configured together"
            )
            .to_owned());
        }
        if !self.cloud_source_roots.is_empty() && self.cloud_source_roots.len() != self.additional_roots.len() {
            return Err("fill_actor.cloud_source_roots must match fill_actor.additional_roots one-for-one".to_owned());
        }
        if self.apply_enabled && !all {
            return Err("fill_actor.apply_enabled requires the CloudDrive move path configuration".to_owned());
        }
        Ok(self)
    }

    fn dump(&self) -> Value {
        object(vec![
            ("actor_root", Value::Str(self.actor_root.clone())),
            ("additional_roots", list_of(&self.additional_roots)),
            ("move_in_root", Value::Str(self.move_in_root.clone())),
            ("move_in_by_brand", Value::Bool(self.move_in_by_brand)),
            ("root_sentinel", Value::Str(self.root_sentinel.clone())),
            ("apply_enabled", Value::Bool(self.apply_enabled)),
            (
                "cloud_strm_mount_prefix",
                Value::Str(self.cloud_strm_mount_prefix.clone()),
            ),
            ("cloud_source_roots", list_of(&self.cloud_source_roots)),
            ("cloud_move_in_root", Value::Str(self.cloud_move_in_root.clone())),
            ("task_dir_path", Value::Str(self.task_dir_path.clone())),
        ])
    }
}

// ---------- merge ----------

/// `MergeConfig`: where the merge tab writes, uploads through and stages a merged title.
/// The defaults match the merge Job the cluster runs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools, reason = "the section's fields, as Python has them")]
pub struct MergeConfig {
    /// Where the Job writes merged files; the app mounts the same directory.
    pub work_dir: String,
    /// The same directory as a CloudDrive API path, for the upload copy.
    pub cloud_work_dir: String,
    /// The 115 directory a merged file is uploaded into and checked in.
    pub cloud_staging_dir: String,
    /// The CloudDrive mount the library's strm files point into.
    pub cloud_mount_prefix: String,
    /// Space a merge leaves free on the work volume beyond the merged file.
    pub free_space_reserve_gib: i64,
    /// Queue titles by themselves, one at a time.
    pub auto_enabled: bool,
    /// Let automatic merging take titles Emby already stacks (cd1-cd9) as well.
    pub auto_include_stackable: bool,
    /// Stop automatic merging when a task fails, instead of skipping that title.
    pub auto_pause_on_failure: bool,
    /// Hand a merge too big for 115 to the home transcode worker.
    pub home_transcode_enabled: bool,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            work_dir: "/downloads/upload".to_owned(),
            cloud_work_dir: "/downloads/upload".to_owned(),
            cloud_staging_dir: "/115/upload".to_owned(),
            cloud_mount_prefix: "/mnt/cd2".to_owned(),
            free_space_reserve_gib: 20,
            auto_enabled: false,
            auto_include_stackable: false,
            auto_pause_on_failure: false,
            home_transcode_enabled: false,
        }
    }
}

impl MergeConfig {
    pub fn configured(&self) -> bool {
        !self.work_dir.is_empty()
            && !self.cloud_work_dir.is_empty()
            && !self.cloud_staging_dir.is_empty()
            && !self.cloud_mount_prefix.is_empty()
    }
}

impl Section for MergeConfig {
    const NAME: &'static str = "merge";
    const MODEL: &'static str = "MergeConfig";
    const FIELDS: &'static [&'static str] = &[
        "work_dir",
        "cloud_work_dir",
        "cloud_staging_dir",
        "cloud_mount_prefix",
        "free_space_reserve_gib",
        "auto_enabled",
        "auto_include_stackable",
        "auto_pause_on_failure",
        "home_transcode_enabled",
    ];

    fn validate_fields(v: &mut Validator<'_>) -> Option<Self> {
        let defaults = Self::default();
        let path = |v: &mut Validator<'_>, name: &'static str, default: String| {
            v.field(
                name,
                || default,
                |v, x| string(v, name, x),
                |value| normalize_absolute_path(&format!("merge.{name}"), &value),
            )
        };
        let work_dir = path(v, "work_dir", defaults.work_dir);
        let cloud_work_dir = path(v, "cloud_work_dir", defaults.cloud_work_dir);
        let cloud_staging_dir = path(v, "cloud_staging_dir", defaults.cloud_staging_dir);
        let cloud_mount_prefix = path(v, "cloud_mount_prefix", defaults.cloud_mount_prefix);
        let free_space_reserve_gib = v.field(
            "free_space_reserve_gib",
            || 20,
            |v, x| integer(v, "free_space_reserve_gib", x),
            |value| {
                if value < 0 {
                    Err("merge.free_space_reserve_gib must not be negative".to_owned())
                } else {
                    Ok(value)
                }
            },
        );
        let flag = |v: &mut Validator<'_>, name: &'static str| v.field(name, || false, |v, x| boolean(v, name, x), Ok);
        let auto_enabled = flag(v, "auto_enabled");
        let auto_include_stackable = flag(v, "auto_include_stackable");
        let auto_pause_on_failure = flag(v, "auto_pause_on_failure");
        let home_transcode_enabled = flag(v, "home_transcode_enabled");
        Some(Self {
            work_dir: work_dir?,
            cloud_work_dir: cloud_work_dir?,
            cloud_staging_dir: cloud_staging_dir?,
            cloud_mount_prefix: cloud_mount_prefix?,
            free_space_reserve_gib: free_space_reserve_gib?,
            auto_enabled: auto_enabled?,
            auto_include_stackable: auto_include_stackable?,
            auto_pause_on_failure: auto_pause_on_failure?,
            home_transcode_enabled: home_transcode_enabled?,
        })
    }

    fn dump(&self) -> Value {
        object(vec![
            ("work_dir", Value::Str(self.work_dir.clone())),
            ("cloud_work_dir", Value::Str(self.cloud_work_dir.clone())),
            ("cloud_staging_dir", Value::Str(self.cloud_staging_dir.clone())),
            ("cloud_mount_prefix", Value::Str(self.cloud_mount_prefix.clone())),
            ("free_space_reserve_gib", Value::from(self.free_space_reserve_gib)),
            ("auto_enabled", Value::Bool(self.auto_enabled)),
            ("auto_include_stackable", Value::Bool(self.auto_include_stackable)),
            ("auto_pause_on_failure", Value::Bool(self.auto_pause_on_failure)),
            ("home_transcode_enabled", Value::Bool(self.home_transcode_enabled)),
        ])
    }
}
