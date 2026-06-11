use postgres::Client;

use crate::config::{TlsConfig, TlsMode};
use crate::error::Result;

use super::{ColumnInfo, TableInfo};

/// Open the one client shared across the whole init run (`list_tables` plus
/// every per-table `introspect` — no per-table reconnect).
///
/// `init` runs before any YAML `tls:` block exists (it *generates* the
/// config), so the transport-security policy comes from the URL's `sslmode`
/// parameter; the connection itself goes through the same
/// [`crate::source::postgres::connect_client`] path as doctor/check/run.
pub(super) fn connect(url: &str) -> Result<Client> {
    let tls = tls_mode_from_url(url).map(|mode| TlsConfig {
        mode,
        ..TlsConfig::default()
    });
    crate::source::postgres::connect_client(url, tls.as_ref())
}

/// Map the URL's `sslmode` query parameter to the [`TlsMode`] the shared TLS
/// connector understands.
///
/// `require` / `verify-ca` / `verify-full` map to the corresponding enforced
/// mode. Everything else — parameter missing, `disable`, `prefer`, `allow`,
/// or an unrecognized value — returns `None` (plaintext), keeping plain dev
/// setups working. [`TlsMode`] has no `prefer` variant, so no try-TLS-then-
/// fallback is attempted; values libpq would reject are left for the driver's
/// own URL parsing to diagnose. Last occurrence wins, matching libpq.
fn tls_mode_from_url(url: &str) -> Option<TlsMode> {
    let (_, query) = url.split_once('?')?;
    let mut mode = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != "sslmode" {
            continue;
        }
        mode = match value {
            "require" => Some(TlsMode::Require),
            "verify-ca" => Some(TlsMode::VerifyCa),
            "verify-full" => Some(TlsMode::VerifyFull),
            _ => None,
        };
    }
    mode
}

/// Tables and views in a PostgreSQL schema (`information_schema`).
pub(super) fn list_tables(client: &mut Client, schema: &str) -> Result<Vec<String>> {
    let rows = client.query(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type IN ('BASE TABLE', 'VIEW')
         ORDER BY table_name",
        &[&schema],
    )?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

pub(super) fn introspect(client: &mut Client, schema: &str, table: &str) -> Result<TableInfo> {
    // Row estimate from pg_class (fast, no COUNT(*))
    let row_estimate: i64 = client
        .query_opt(
            "SELECT reltuples::bigint FROM pg_class
             WHERE relname = $1 AND relnamespace = (
                 SELECT oid FROM pg_namespace WHERE nspname = $2
             )",
            &[&table, &schema],
        )?
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0)
        .max(0);

    // Physical size (heap + indexes); None for views and when privileges are missing.
    let total_bytes: Option<i64> = client
        .query_opt(
            "SELECT pg_total_relation_size(c.oid)::bigint
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = $1 AND n.nspname = $2 AND c.relkind IN ('r','p','m')",
            &[&table, &schema],
        )
        .ok()
        .flatten()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .filter(|v| *v > 0);

    // Primary key columns
    // to_regclass() returns NULL (no error) when the table disappears between
    // list_tables and introspect — possible under concurrent test table drops.
    let pk_rows = client.query(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid
             AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = to_regclass($1 || '.' || $2)
           AND i.indrelid IS NOT NULL
           AND i.indisprimary",
        &[&schema, &table],
    )?;
    let pk_cols: std::collections::HashSet<String> =
        pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    // Column metadata — including NULL-ability and numeric precision/scale for decimal columns.
    let col_rows = client.query(
        "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
         FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position",
        &[&schema, &table],
    )?;

    if col_rows.is_empty() {
        anyhow::bail!(
            "Table '{schema}.{table}' not found or has no columns. \
             Check the table name and that the user has SELECT privilege."
        );
    }

    let columns = col_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let is_nullable_str: String = row.get(2);
            let numeric_precision: Option<i32> = row.get(3);
            let numeric_scale: Option<i32> = row.get(4);
            let is_primary_key = pk_cols.contains(&name);
            ColumnInfo {
                name,
                data_type,
                is_primary_key,
                is_nullable: is_nullable_str.eq_ignore_ascii_case("YES"),
                numeric_precision: numeric_precision.map(|v| v as u32),
                numeric_scale: numeric_scale.map(|v| v as u32),
            }
        })
        .collect();

    Ok(TableInfo {
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        total_bytes,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::tls_mode_from_url;
    use crate::config::TlsMode;

    #[test]
    fn sslmode_enforced_values_map_to_tls_modes() {
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host:5432/db?sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=verify-ca"),
            Some(TlsMode::VerifyCa)
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=verify-full"),
            Some(TlsMode::VerifyFull)
        );
    }

    #[test]
    fn sslmode_plaintext_values_stay_plaintext() {
        // Missing, disable, and libpq's plaintext-leaning modes (prefer/allow)
        // all keep the current NoTls behavior — TlsMode has no `prefer`.
        assert_eq!(tls_mode_from_url("postgresql://u:p@host/db"), None);
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=disable"),
            None
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=prefer"),
            None
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=allow"),
            None
        );
    }

    #[test]
    fn sslmode_unrecognized_values_stay_plaintext() {
        // libpq values are lowercase and exact; the driver rejects anything
        // else at connect time, so derivation must not guess.
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=REQUIRE"),
            None
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=garbage"),
            None
        );
        assert_eq!(tls_mode_from_url("postgresql://u:p@host/db?sslmode"), None);
        assert_eq!(tls_mode_from_url("postgresql://u:p@host/db?sslmode="), None);
    }

    #[test]
    fn sslmode_found_among_other_params_and_exact_key_only() {
        assert_eq!(
            tls_mode_from_url(
                "postgresql://u:p@host/db?connect_timeout=10&sslmode=require&application_name=x"
            ),
            Some(TlsMode::Require)
        );
        // Key must match exactly — `xsslmode` is a different parameter.
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?xsslmode=require"),
            None
        );
    }

    #[test]
    fn sslmode_last_occurrence_wins() {
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=disable&sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            tls_mode_from_url("postgresql://u:p@host/db?sslmode=require&sslmode=disable"),
            None
        );
    }
}
