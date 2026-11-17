//! The database-only scenarios (`tools/scenarios/specs/db/`, `config/`): the Rust ports of
//! `tools/scenarios/operations/db.py` and `config.py`, compared with the Python transcripts.

use chrono::DateTime;
use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::{self, Parts, Seeded, Spec};
use sqlx::{AssertSqlSafe, PgPool};

use super::{dump_rows, seed, unseeded_fakes};
use crate::config_store::{ConfigStore, ConfigUpdateError, Versioned};
use crate::database::{Database, DatabaseError};
use crate::migrations;
use crate::testdb::TestDatabase;

fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut dict = Dict::new();
    for (key, value) in entries {
        dict.insert(key, value);
    }
    Value::Dict(dict)
}

/// Run one scenario: seed, run `operation` on the scenario's database, compare.
async fn run<F, Fut>(name: &str, operation: F)
where
    F: FnOnce(Spec, TestDatabase) -> Fut,
    Fut: Future<Output = (Spec, TestDatabase, Value)>,
{
    let spec = Spec::load(name);
    let test = TestDatabase::create().await;
    seed(test.pool(), &spec).await;
    let (spec, test, outcome) = operation(spec, test).await;
    let seeded = Seeded::new(&spec);
    let rows = dump_rows(test.pool(), &spec).await;
    let fakes = unseeded_fakes(&spec);
    scenario::assert_transcript(Parts {
        spec: &spec,
        seeded: &seeded,
        outcome,
        calls: Vec::new(),
        rows,
        fakes,
    });
    test.drop_database().await;
}

async fn versions(pool: &PgPool) -> Value {
    let versions: Vec<i32> = sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .expect("versions");
    object(vec![(
        "versions",
        Value::List(versions.into_iter().map(|version| Value::Int(version.into())).collect()),
    )])
}

/// `db._open`: open the database as the application does, then close it.
async fn open(test: &TestDatabase) -> Result<(), DatabaseError> {
    let database: Database = test.database();
    let result = database.get_pool().await.map(|_| ());
    database.close().await;
    result
}

fn database_error(error: &DatabaseError) -> Value {
    match error {
        DatabaseError::UnsupportedSchemaVersion(_) => {
            scenario::error("UnsupportedSchemaVersionError", &error.to_string())
        }
        other => panic!("unexpected database error: {other}"),
    }
}

#[tokio::test]
async fn db_schema_matches_the_python_transcript() {
    run("db/schema", |spec, test| async move {
        let query = spec
            .section("args")
            .get("catalog")
            .and_then(Value::as_str)
            .expect("catalog")
            .to_owned();
        // The catalog query is the spec's own SQL.
        let text: String = sqlx::query_scalar(AssertSqlSafe(query))
            .fetch_one(test.pool())
            .await
            .expect("catalog");
        let outcome = scenario::result(pyjson::loads(&text).expect("JSON"));
        (spec, test, outcome)
    })
    .await;
}

#[tokio::test]
async fn db_newer_schema_refused_matches_the_python_transcript() {
    run("db/newer_schema_refused", |spec, test| async move {
        let outcome = match open(&test).await {
            Ok(()) => scenario::result(versions(test.pool()).await),
            Err(error) => database_error(&error),
        };
        (spec, test, outcome)
    })
    .await;
}

#[tokio::test]
async fn db_migrate_from_v4_matches_the_python_transcript() {
    run("db/migrate_from_v4", |spec, test| async move {
        let args = spec.section("args");
        let from = i32::try_from(golden_int(args.get("from_version").expect("from_version"))).expect("small");
        let now = DateTime::parse_from_rfc3339(spec.clock()).expect("clock").to_utc();
        let mut transaction = test.pool().begin().await.expect("begin");
        sqlx::raw_sql(
            "
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL
)
",
        )
        .execute(&mut *transaction)
        .await
        .expect("schema_migrations");
        for applied in 1..=from {
            for statement in migrations::statements(applied) {
                sqlx::raw_sql(AssertSqlSafe(*statement))
                    .execute(&mut *transaction)
                    .await
                    .expect("statement");
            }
            sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)")
                .bind(applied)
                .bind(now)
                .execute(&mut *transaction)
                .await
                .expect("version");
        }
        transaction.commit().await.expect("commit");
        for statement in args.get("seed").and_then(Value::as_list).unwrap_or_default() {
            let statement = statement.as_str().expect("SQL").to_owned();
            sqlx::raw_sql(AssertSqlSafe(statement))
                .execute(test.pool())
                .await
                .expect("seed");
        }
        let outcome = match open(&test).await {
            Ok(()) => scenario::result(versions(test.pool()).await),
            Err(error) => database_error(&error),
        };
        (spec, test, outcome)
    })
    .await;
}

fn golden_int(value: &Value) -> i128 {
    match value {
        Value::Int(int) => *int,
        other => panic!("not an integer: {other:?}"),
    }
}

fn snapshot(sections: &[Versioned]) -> Value {
    Value::List(
        sections
            .iter()
            .map(|entry| {
                object(vec![
                    ("section", Value::from(entry.section.name())),
                    ("version", Value::Int(entry.version.into())),
                    ("dump_json", Value::Str(entry.section.dump_json())),
                ])
            })
            .collect(),
    )
}

/// `config._failure`.
fn failure(error: &ConfigUpdateError) -> Value {
    match error {
        ConfigUpdateError::Invalid(invalid) => object(vec![
            ("type", Value::from("ValidationError")),
            (
                "errors",
                Value::List(
                    invalid
                        .errors
                        .iter()
                        .map(|detail| {
                            object(vec![
                                ("type", Value::from(detail.kind)),
                                ("loc", Value::List(detail.loc.clone())),
                                ("msg", Value::Str(detail.msg.clone())),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]),
        ConfigUpdateError::VersionConflict(conflict) => object(vec![
            ("type", Value::from("ConfigVersionConflictError")),
            ("message", Value::Str(conflict.to_string())),
        ]),
        ConfigUpdateError::UnknownSection(unknown) => object(vec![
            ("type", Value::from("UnknownConfigSectionError")),
            ("message", Value::Str(unknown.to_string())),
        ]),
        ConfigUpdateError::Database(error) => panic!("database error: {error}"),
    }
}

#[tokio::test]
async fn config_store_matches_the_python_transcript() {
    run("config/store", |spec, test| async move {
        let database = test.database();
        let store = ConfigStore::new(database.clone());
        store.load().await.expect("load");
        let loaded = snapshot(&store.sections());
        let mut results = Vec::new();
        for update in spec
            .section("args")
            .get("updates")
            .and_then(Value::as_list)
            .unwrap_or_default()
        {
            let section = update.get("section").and_then(Value::as_str).expect("section");
            let values = update.get("values").and_then(Value::as_dict).expect("values");
            let expected = update
                .get("expected_version")
                .map(|value| i32::try_from(golden_int(value)).expect("small"));
            results.push(match store.update(section, values, expected).await {
                Ok(entry) => {
                    let flags: Dict = entry
                        .section
                        .secret_flags()
                        .into_iter()
                        .map(|(name, flag)| (name.to_owned(), Value::Bool(flag)))
                        .collect();
                    object(vec![
                        ("version", Value::Int(entry.version.into())),
                        ("dump_json", Value::Str(entry.section.dump_json())),
                        ("masked", entry.section.masked_values()),
                        ("secret_flags", Value::Dict(flags)),
                    ])
                }
                Err(error) => object(vec![("error", failure(&error))]),
            });
        }
        let outcome = scenario::result(object(vec![
            ("loaded", loaded),
            ("updates", Value::List(results)),
            ("final", snapshot(&store.sections())),
        ]));
        database.close().await;
        (spec, test, outcome)
    })
    .await;
}
