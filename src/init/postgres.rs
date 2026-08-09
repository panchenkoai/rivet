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
pub(super) fn connect(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    // An explicit `--tls` flag WINS over the URL's `sslmode` — the flag is the
    // operator's direct statement, the URL parameter an inherited convention.
    let from_url;
    let tls = match tls {
        Some(t) => Some(t),
        None => {
            from_url = tls_from_url(url);
            from_url.as_ref()
        }
    };
    crate::source::postgres::connect_client(url, tls)
}

/// The URL's `sslmode`, as the [`TlsConfig`] the connection actually uses.
///
/// Split from `connect` (which needs a live server) so the CONSTRUCTION is
/// unit-testable: the mutation gate showed `mode` could be dropped from this
/// struct and nothing offline noticed — the URL's posture would silently fall
/// back to the default.
fn tls_from_url(url: &str) -> Option<TlsConfig> {
    tls_mode_from_url(url).map(|mode| TlsConfig {
        mode,
        ..TlsConfig::default()
    })
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
        density: None,
        schema: schema.to_string(),
        table: table.to_string(),
        row_estimate,
        total_bytes,
        columns,
    })
}

/// First-run density probe (#148), PostgreSQL leg: `reltuples` after
/// autovacuum is usually within a few %, so the probe runs only on LARGE
/// tables (> 5M estimated), where a % error still moves absolute decisions
/// (parallel scaling, duration prediction). Same stratified method as MySQL;
/// best-effort (any error keeps the catalog figure, marked unverified only
/// when we attempted and failed — an un-probed small table keeps density=None,
/// i.e. "the catalog is trusted here by policy").
pub(super) fn density_probe(client: &mut Client, info: &mut super::TableInfo) {
    use super::density::*;

    const PG_PROBE_LINE: i64 = 5_000_000;
    let catalog = info.row_estimate;
    if catalog < PG_PROBE_LINE {
        return; // policy: reltuples trusted below the line (density stays None)
    }
    let Some(key) = info.best_chunk_column().map(str::to_string) else {
        info.density = Some(DensityProbe {
            rows: catalog,
            density: 0.0,
            method: EstimateMethod::Unverified,
            catalog_rows: catalog,
            k: 0,
            w: 0,
        });
        return;
    };
    let q_ident = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let rel = format!("{}.{}", q_ident(&info.schema), q_ident(&info.table));
    let kq = q_ident(&key);
    let Ok(row) = client.query_one(
        &format!("SELECT MIN({kq})::bigint, MAX({kq})::bigint FROM {rel}"),
        &[],
    ) else {
        return;
    };
    let (Some(min), Some(max)): (Option<i64>, Option<i64>) = (row.get(0), row.get(1)) else {
        return;
    };
    let offsets = stratified_offsets(min, max, PROBE_K, PROBE_W);
    if offsets.is_empty() {
        return;
    }
    let mut counts = Vec::with_capacity(offsets.len());
    for off in &offsets {
        let hi = off.saturating_add(PROBE_W - 1);
        match client.query_one(
            &format!("SELECT COUNT(*) FROM {rel} WHERE {kq} BETWEEN {off} AND {hi}"),
            &[],
        ) {
            Ok(r) => counts.push(r.get::<_, i64>(0)),
            Err(_) => return,
        }
    }
    let (rows, density) = estimate_from_windows(min, max, &counts, PROBE_W);
    info.row_estimate = rows;
    info.density = Some(DensityProbe {
        rows,
        density,
        method: EstimateMethod::Probed,
        catalog_rows: catalog,
        k: offsets.len(),
        w: PROBE_W,
    });
}

#[cfg(test)]
mod tests {
    /// The struct the URL's sslmode becomes must CARRY the mode — dropping the
    /// field compiles (struct-update syntax fills the default) and silently
    /// downgrades the posture. Named by the mutation gate on #154.
    #[test]
    fn url_sslmode_lands_in_the_config_it_builds() {
        use crate::config::TlsMode;
        // `require`, deliberately NOT `verify-full`: VerifyFull is TlsMode's
        // `#[default]`, so a fixture equal to the default cannot see the
        // delete-field mutant (struct-update fills the default back in) — the
        // below-activation-threshold fixture lesson, caught on the first try.
        let t = super::tls_from_url("postgresql://u:p@h/db?sslmode=require").expect("require maps");
        assert_eq!(t.mode, TlsMode::Require);
        assert!(!t.accept_invalid_certs && t.ca_file.is_none());
        assert!(super::tls_from_url("postgresql://u:p@h/db").is_none());
    }

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
