//! The database half of the effect transcripts (docs/rewrite/SCENARIOS.md).
//!
//! Mirrors `tools/scenarios/runner.py`: [`seed`] runs the spec's `database.sql` statements
//! (and, from WP1 on, the application schema), [`dump_rows`] reads every dumped table as
//! PostgreSQL's own `to_jsonb` renders it, so both builds compare rows without a type
//! mapping of their own.

use embyx_core::pyjson::{self, Dict, Value};
use embyx_core::scenario::Spec;
use sqlx::{AssertSqlSafe, PgPool};

/// `runner.DUMP_SQL`.
const DUMP_SQL: &str = "
SELECT COALESCE(jsonb_agg(row_value ORDER BY row_value::text), '[]'::jsonb)::text
FROM (SELECT to_jsonb(t) AS row_value FROM {table} AS t) AS dumped
";

/// `runner.TABLES_SQL`.
const TABLES_SQL: &str = "
SELECT table_name::text FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name <> 'schema_migrations'
ORDER BY table_name
";

/// `runner.seed_database`. `schema = "app"` needs the migrations, which arrive in WP1.
pub async fn seed(pool: &PgPool, spec: &Spec) {
    let database = spec.section("database");
    let schema = database.get("schema").and_then(Value::as_str).unwrap_or("none");
    assert_eq!(
        schema, "none",
        "database.schema = {schema:?} needs the application migrations (WP1)"
    );
    for statement in database.get("sql").and_then(Value::as_list).unwrap_or_default() {
        let statement = statement.as_str().expect("SQL statements are strings").to_owned();
        // Seed SQL is written by the scenario author, never built from data.
        sqlx::raw_sql(AssertSqlSafe(statement))
            .execute(pool)
            .await
            .expect("seed statement");
    }
}

/// `runner.dump_rows`: `{table: [row, ...]}` for the spec's tables, or every public table.
pub async fn dump_rows(pool: &PgPool, spec: &Spec) -> Value {
    let mut connection = pool.acquire().await.expect("connection");
    sqlx::raw_sql("SET TimeZone TO 'UTC'")
        .execute(&mut *connection)
        .await
        .expect("time zone");
    let named: Vec<String> = spec
        .section("database")
        .get("tables")
        .and_then(Value::as_list)
        .unwrap_or_default()
        .iter()
        .map(|table| table.as_str().expect("table names are strings").to_owned())
        .collect();
    let tables = if named.is_empty() {
        sqlx::query_scalar::<_, String>(TABLES_SQL)
            .fetch_all(&mut *connection)
            .await
            .expect("tables")
    } else {
        named
    };
    let mut rows = Dict::new();
    for table in tables {
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        // The table name is quoted above; the statement is the runner's.
        let text: String = sqlx::query_scalar(AssertSqlSafe(DUMP_SQL.replace("{table}", &quoted)))
            .fetch_one(&mut *connection)
            .await
            .expect("dump");
        rows.insert(table, pyjson::loads(&text).expect("PostgreSQL's JSON parses"));
    }
    Value::Dict(rows)
}
