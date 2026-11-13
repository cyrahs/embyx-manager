//! The versioned configuration store backed by the `app_config` table.
//!
//! Python origin: `src/embyx_manager/config/store.py`. Reads are served from an in-process
//! snapshot so hot paths never wait on the database; the snapshot is refreshed after local
//! writes at once and through [`ConfigStore::refresh`] on an interval, so other replicas
//! converge within seconds. Updates use optimistic version checks. A stored section that no
//! longer validates is logged and the previous value kept (contract 2: both builds read and
//! write the same `value_json` bytes, `goldens/config/`).

use std::fmt;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Utc;
use embyx_core::config::{AnySection, Mode, Registered, SECTION_NAMES, ValidationError};
use embyx_core::pyjson::{Dict, Value};
use embyx_core::pytext::py_repr;

use crate::database::{Database, DatabaseError};

/// `ConfigVersionConflictError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("config section {section} version conflict: expected {expected}, stored {actual}")]
pub struct ConfigVersionConflictError {
    pub section: String,
    pub expected: i32,
    pub actual: i32,
}

/// `UnknownConfigSectionError` (a `KeyError`, whose text is the key's `repr`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownConfigSectionError {
    pub section: String,
}

impl fmt::Display for UnknownConfigSectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&py_repr(&self.section))
    }
}

impl std::error::Error for UnknownConfigSectionError {}

/// Why [`ConfigStore::update`] refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum ConfigUpdateError {
    #[error(transparent)]
    UnknownSection(#[from] UnknownConfigSectionError),
    #[error(transparent)]
    VersionConflict(#[from] ConfigVersionConflictError),
    /// The merged values (or the stored row) failed the section's validation.
    #[error(transparent)]
    Invalid(#[from] ValidationError),
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

impl From<sqlx::Error> for ConfigUpdateError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(DatabaseError::Sqlx(error))
    }
}

/// One section of the snapshot: its value and stored version (0 when never written).
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned {
    pub section: AnySection,
    pub version: i32,
}

/// `ConfigStore`.
#[derive(Debug)]
pub struct ConfigStore {
    database: Database,
    snapshot: RwLock<Vec<Versioned>>,
    write_lock: tokio::sync::Mutex<()>,
    loaded: AtomicBool,
}

fn defaults() -> Vec<Versioned> {
    SECTION_NAMES
        .iter()
        .map(|name| Versioned {
            section: AnySection::default_for(name).expect("registered"),
            version: 0,
        })
        .collect()
}

fn position(name: &str) -> Option<usize> {
    SECTION_NAMES.iter().position(|known| *known == name)
}

impl ConfigStore {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            snapshot: RwLock::new(defaults()),
            write_lock: tokio::sync::Mutex::new(()),
            loaded: AtomicBool::new(false),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<Versioned>> {
        self.snapshot.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<Versioned>> {
        self.snapshot.write().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `get(SectionType)`: the section's current value from the snapshot.
    pub fn get<S: Registered>(&self) -> S {
        let index = position(S::NAME).expect("registered section");
        S::unwrap(&self.read()[index].section)
            .expect("the registry keeps types aligned")
            .clone()
    }

    /// `get_with_version(section)`.
    pub fn get_with_version(&self, section: &str) -> Result<Versioned, UnknownConfigSectionError> {
        let index = position(section).ok_or_else(|| UnknownConfigSectionError {
            section: section.to_owned(),
        })?;
        Ok(self.read()[index].clone())
    }

    /// `sections()`: every section with its version, in registry order.
    pub fn sections(&self) -> Vec<Versioned> {
        self.read().clone()
    }

    /// `refresh()`: reload every section from the database into the snapshot.
    pub async fn refresh(&self) -> Result<(), DatabaseError> {
        let pool = self.database.get_pool().await?;
        let rows: Vec<(String, String, i32)> = sqlx::query_as("SELECT section, value_json, version FROM app_config")
            .fetch_all(&pool)
            .await?;
        let mut snapshot = self.write();
        for (index, name) in SECTION_NAMES.iter().enumerate() {
            let Some((_, value_json, version)) = rows.iter().find(|(section, _, _)| section == name) else {
                snapshot[index] = Versioned {
                    section: AnySection::default_for(name).expect("registered"),
                    version: 0,
                };
                continue;
            };
            match AnySection::validate_json(name, value_json).expect("registered") {
                Ok(section) => {
                    snapshot[index] = Versioned {
                        section,
                        version: *version,
                    };
                }
                Err(error) => {
                    tracing::error!(
                        section = name,
                        %error,
                        "stored config for section {name} is invalid; keeping previous value"
                    );
                }
            }
        }
        drop(snapshot);
        self.loaded.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// `load()`: [`refresh`](Self::refresh) once.
    pub async fn load(&self) -> Result<(), DatabaseError> {
        if !self.loaded.load(Ordering::SeqCst) {
            self.refresh().await?;
        }
        Ok(())
    }

    /// `update(section, values, expected_version=...)`: merge `values` onto the stored
    /// section and persist it. An empty or `null` secret keeps the stored secret.
    pub async fn update(
        &self,
        section: &str,
        values: &Dict,
        expected_version: Option<i32>,
    ) -> Result<Versioned, ConfigUpdateError> {
        let index = position(section).ok_or_else(|| UnknownConfigSectionError {
            section: section.to_owned(),
        })?;
        let name = SECTION_NAMES[index];
        let _write = self.write_lock.lock().await;
        let pool = self.database.get_pool().await?;
        let mut transaction = pool.begin().await?;
        let row: Option<(String, i32)> =
            sqlx::query_as("SELECT value_json, version FROM app_config WHERE section = $1 FOR UPDATE")
                .bind(name)
                .fetch_optional(&mut *transaction)
                .await?;
        let (current, current_version) = match row {
            Some((value_json, version)) => (
                AnySection::validate_json(name, &value_json).expect("registered")?,
                version,
            ),
            None => (AnySection::default_for(name).expect("registered"), 0),
        };
        if let Some(expected) = expected_version
            && expected != current_version
        {
            return Err(ConfigVersionConflictError {
                section: name.to_owned(),
                expected,
                actual: current_version,
            }
            .into());
        }
        let Value::Dict(mut merged) = current.dump() else {
            unreachable!("a section dumps as an object");
        };
        let secrets = current.secret_fields();
        for (key, value) in values.iter() {
            let blank = matches!(value, Value::Null) || matches!(value, Value::Str(text) if text.is_empty());
            if blank && key.as_str().is_some_and(|key| secrets.contains(&key)) {
                continue; // keep the stored secret
            }
            merged.insert(key.clone(), value.clone());
        }
        let updated = AnySection::validate(name, &Value::Dict(merged), Mode::Python).expect("registered")?;
        let new_version = current_version + 1;
        sqlx::query(
            "
                    INSERT INTO app_config (section, value_json, version, updated_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (section) DO UPDATE SET
                        value_json = excluded.value_json,
                        version = excluded.version,
                        updated_at = excluded.updated_at
                    ",
        )
        .bind(name)
        .bind(updated.dump_json())
        .bind(new_version)
        .bind(Utc::now())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let entry = Versioned {
            section: updated,
            version: new_version,
        };
        self.write()[index] = entry.clone();
        Ok(entry)
    }
}

#[cfg(test)]
mod tests;
