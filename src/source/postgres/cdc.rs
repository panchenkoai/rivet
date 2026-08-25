//! PostgreSQL CDC adapter — logical replication slot → canonical
//! [`cdc::ChangeEvent`].
//!
//! Consumes a `test_decoding` slot via `pg_logical_slot_get_changes()` with the
//! sync `postgres` crate rivet already depends on — the *poll* model (no
//! streaming-protocol crate; `START_REPLICATION` needs the immature
//! `pg_walstream`/`pgwire-replication` ecosystem). `next_change` polls the slot
//! once into a buffer and drains it; a continuous daemon wraps [`crate::source::cdc::run`]
//! in an outer poll loop.
//!
//! Pre-images / typed values are deferred — like MySQL was before its typed pass,
//! this adapter carries op + schema + table + position; the `test_decoding`
//! payload parse into typed before/after is the PostgreSQL completion step.
//!
//! Prereqs: `wal_level = logical`, a role with `REPLICATION`, `pg_hba.conf`
//! allowing it. Caveat: a logical slot **pins WAL** until consumed — an abandoned
//! slot fills the disk.
//!
//! `#![allow(dead_code)]`: consumed by `cli::dispatch` (binary crate); the lib
//! crate compiles `source` for tests but has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::VecDeque;

use postgres::{Client, NoTls};
use serde_json::json;

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::require_tls_or_loopback;

/// Polls a logical slot and yields canonical changes.
pub(crate) struct PgChangeStream {
    client: Client,
    slot: String,
    pending: VecDeque<ChangeEvent>,
    /// Wire budget per `peek` — the memory bound of the drain (O(batch), not
    /// O(total backlog)). One ack cadence (the part rollover); see
    /// [`wire_budget`]. Slot progress past a foreign/empty span larger than one
    /// window is NOT this budget's job — it comes from the sink's re-drain loop
    /// acking the consumed span so the next peek slides forward (see
    /// [`crate::source::cdc::sink::run_to_files`]).
    batch_limit: i32,
    /// Largest COMMIT LSN already yielded THIS run. A refill re-peeks from the
    /// slot's (un-acked) `restart_lsn`, so any transaction at/below this was
    /// already delivered — it is dropped, making the refill idempotent.
    frontier: u64,
    /// A peek that yields no NEW transaction, or returns fewer than a full
    /// batch, has drained everything readable *from the current slot position*.
    /// It is NOT terminal for an acking consumer: [`ChangeStream::ack`] (and the
    /// zero-yield [`Self::release_empty_frontier`]) advance the slot and clear
    /// this, so the sink's re-drain loop peeks fresh WAL past a consumed
    /// foreign/empty span. Only a non-acking consumer (NDJSON, one big
    /// `Unbounded` peek) treats it as the end.
    exhausted: bool,
    /// A TRUNCATE of a captured table, seen but NOT yet raised.
    ///
    /// Raising it the instant the line is scanned discards everything already read
    /// in the same peek window: the run fails with `rows: 0`, so nothing flushes,
    /// nothing acks, and the slot has not moved. The refusal's own remedy —
    /// re-snapshot, then `pg_replication_slot_advance` past the truncate — then
    /// throws those transactions away. MEASURED: a two-table export where
    /// `public.tb` committed an insert BEFORE `public.ta` was truncated lost that
    /// insert entirely when the remedy was followed verbatim; `tb` was never
    /// truncated and the message names only `ta`.
    ///
    /// So the truncate ENDS the window instead (`exhausted`), the sink flushes +
    /// checkpoints + acks everything that precedes it, and the refusal is raised on
    /// the next `fill`. The slot then sits exactly at the last commit before the
    /// truncate, which is what makes "advance past it" lossless for everything else.
    pending_truncate_refusal: Option<String>,
    /// Open-time COMMIT-LSN ceiling for a bounded run — the first transaction
    /// committing past it ends the stream; `None` (daemon / anchor-only open)
    /// keeps the pure catch-up exit. The contract lives on [`DrainMode`].
    bound: Option<u64>,
    /// Any DATA event pushed this run. When still `false` at clean exhaust,
    /// every frontier-covered transaction was EMPTY (DDL churn decodes as
    /// row-less BEGIN/COMMIT) — the sink has nothing to flush, so it never
    /// acks, and the slot would pin WAL behind the noise forever on an idle
    /// database. A zero-yield run releases the span itself
    /// ([`Self::release_empty_frontier`]): advancing past a data-free span can
    /// lose nothing by construction.
    yielded_data: bool,
    /// Rendered LSN of the last frontier advance — the zero-yield release
    /// target. `take()`n once at exhaust.
    frontier_text: Option<String>,
    /// The `table:` values this run captures — what a TRUNCATE is checked against
    /// before it is allowed to fail the run. See [`truncate_is_ours`].
    configured_tables: Vec<String>,
}

/// The run-start warning emitted when a logical replication slot has to be
/// CREATED — i.e. capture starts at the current WAL position and nothing written
/// before now is reachable.
///
/// `warn`, never `info`: the default log level hides `info`, so an info-level
/// "this may have skipped your data" is functionally silent — the same rule the
/// sparse-chunk diagnostic was fixed under. Pure and unit-tested because the
/// branch that emits it needs a live PostgreSQL, so the message itself is the
/// only part an offline test can hold.
pub(crate) fn slot_created_warning(slot: &str) -> String {
    format!(
        "pg cdc: creating replication slot '{slot}' — capture starts at the CURRENT WAL \
         position, so changes written before now are NOT captured. On a first run this is \
         expected. If this slot existed before, it was dropped or invalidated and the changes \
         since then are unrecoverable: re-snapshot (mode: full) before trusting this stream. \
         Set `cdc.checkpoint:` to turn this case into a hard error instead of a warning."
    )
}

/// What one CONFIGURED table can contribute to a `test_decoding` stream —
/// the pure half of [`PgChangeStream::check_configured_tables_are_routable`].
///
/// `test_decoding` names the relation a row PHYSICALLY landed in, and the sink
/// routes by byte-exact comparison against the config's `table:` string. When
/// those two can never agree, capture is a silent 100% drop — and on PostgreSQL
/// that is terminal, not a delay: the commit boundary is recorded BEFORE the
/// routing filter (`cdc/sink.rs`), so the pass checkpoints and acks the slot over
/// events it never captured, PostgreSQL frees the WAL, and the run writes
/// `_SUCCESS` with `status: success` and zero rows.
///
/// Measured on a real partitioned table (2026-08-24, pg14 stand): 2 committed
/// rows, `rows: 0`, slot `0/AE5F8BB8` → `0/AE6010D8`, peek empty afterwards, and
/// re-running with the config corrected recovered NOTHING while the source still
/// held both rows. That is worse than the SQL Server case this guard is modelled
/// on (`mssql/cdc.rs`), where the change table's own retention made the same
/// routing bug a delay.
pub(crate) struct RelationRouting<'a> {
    /// The `table:` string the config asked for — what the sink routes BY.
    pub configured: &'a str,
    /// `pg_class.relkind` of the relation that name resolves to.
    pub relkind: char,
    /// `pg_class.relpersistence` — `u` for UNLOGGED. An unlogged table writes no
    /// WAL, so logical decoding never emits a single event for it.
    pub relpersistence: char,
    /// The CATALOG's spelling of the relation the config string resolved to, as
    /// `(schema, table)`. The probe resolves through SQL rules (which FOLD an
    /// unquoted name); the router compares BYTE-EXACT. When those two disagree the
    /// config means one relation to the probe and another to routing.
    pub resolved: (String, String),
    /// Relations that inherit from it — partitions, or legacy `INHERITS`
    /// children — schema-qualified the way `test_decoding` renders them.
    pub children: Vec<String>,
    /// For a partitioned parent: the LEAF partitions, from `pg_partition_tree`.
    ///
    /// Not the same as `children` on a sub-partitioned table, and the difference is
    /// a dead-end remediation: `children` there is the intermediate PARTITIONED
    /// table, which stores no rows either and is refused by this same guard on the
    /// next run. Only a leaf ever appears in the WAL.
    pub leaves: Vec<String>,
}

/// The verdict for one configured table. `Never` is a hard error (a guaranteed
/// total, unrecoverable drop); `Partial` is a warning, because the parent's OWN
/// rows do route and refusing would break a working config.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RoutingVerdict {
    Routable,
    Never(String),
    Partial(String),
}

/// Classify one configured relation. Pure: the catalog read that fills
/// [`RelationRouting`] needs a live PostgreSQL, so this — every decision the
/// guard makes — is what an offline test can hold and mutants can grade.
pub(crate) fn classify_routing(rel: &RelationRouting<'_>) -> RoutingVerdict {
    let cfg = rel.configured;
    let (rs, rt) = (&rel.resolved.0, &rel.resolved.1);

    // FIRST, because it is the case where the schema is resolved from one relation
    // and the events come from another. The config string is read by two resolvers
    // with different case rules: the schema probe interpolates it into
    // `SELECT * FROM {table}` and lets PostgreSQL FOLD it, while the sink routes
    // byte-exact against the unquoted wire identity. They agree for an ordinary
    // lowercase name and diverge the moment a folded twin exists.
    //
    // MEASURED (adversarial pass, 2026-08-24) with BOTH `"MixedCase"(id,v)` and
    // `mixedcase(id,other_col,extra)` present and `table: MixedCase` configured:
    // exit 0 and `status: success` throughout, while (a) writes to `mixedcase` —
    // the relation the probe actually resolved — captured `rows: 0` with no
    // warning, and (b) writes to `"MixedCase"` were written under the WRONG
    // table's schema: columns `id, other_col, extra`, the real `v` values absent
    // entirely and no `v` column in the output. Silent column loss on top of
    // silent event loss.
    //
    // This also catches the 3-part `db.schema.table` form: PostgreSQL's
    // `to_regclass` accepts it when `db` is the current database, so the probe
    // resolves happily while `table_matches` splits on the FIRST dot and compares
    // `db` against the schema — never matching.
    if !crate::source::cdc::sink::table_matches(cfg, rs, rt) {
        return RoutingVerdict::Never(format!(
            "pg cdc: `{cfg}` resolves to the relation `{rs}.{rt}`, but routing compares the              config string BYTE-EXACT against the name test_decoding emits — and those two              disagree here. The schema probe would read `{rs}.{rt}`'s columns while events              carrying a different spelling route nowhere, so the run would either capture              NOTHING or write rows under the wrong table's schema, silently, past an              advancing slot. Set `table:` to `{rs}.{rt}` exactly as the catalog spells it."
        ));
    }

    // An UNLOGGED table writes no WAL at all, so no logical-decoding event can ever
    // exist for it — the same total, silent drop as a view, by a different route.
    if rel.relpersistence == 'u' {
        return RoutingVerdict::Never(format!(
            "pg cdc: `{cfg}` is an UNLOGGED table. Unlogged tables write no WAL, so logical              decoding never produces a single event for them and the capture would report              success with zero rows forever while the slot advanced past other traffic.              `ALTER TABLE {rs}.{rt} SET LOGGED` to capture it, or drop it from the config."
        ));
    }

    match rel.relkind {
        // A partitioned parent stores NO rows of its own: every change carries a
        // PARTITION's name. Byte-exact routing can never match it.
        'p' => {
            // LEAVES, not children. On a sub-partitioned table the immediate child
            // is another partitioned table that stores no rows, so naming it sends
            // the operator to a config this same guard refuses on the next run.
            let named = if rel.leaves.is_empty() {
                "it has no partitions yet".to_string()
            } else {
                format!("currently {}", rel.leaves.join(", "))
            };
            RoutingVerdict::Never(format!(
                "pg cdc: `{cfg}` is a PARTITIONED table — it stores no rows itself, and \
                 test_decoding names the PARTITION a row landed in, never the parent. Routing is \
                 byte-exact, so every change would be dropped AND the slot would advance past it: \
                 PostgreSQL frees the WAL and the changes are gone from the log and the \
                 destination alike, while the run reports success with 0 rows. Capture the \
                 partitions by name ({named}) — a partition added later needs its own entry — or \
                 snapshot the parent with `mode: full`, which reads through the parent normally."
            ))
        }
        // A MATERIALIZED VIEW is the one of the three that CAN emit under its own
        // name, so it is a partial, not a refusal. MEASURED on the pg16 CDC stand,
        // three forms against one `test_decoding` slot:
        //   CREATE MATERIALIZED VIEW … WITH DATA      → `table s.mv: INSERT: …`
        //   REFRESH MATERIALIZED VIEW …               → nothing under its own name
        //   REFRESH MATERIALIZED VIEW CONCURRENTLY …  → `table s.mv: INSERT: …`
        // The earlier comment here measured only the middle form and generalised it
        // to "a guaranteed zero" — while the candidate query 200 lines below was
        // rewritten on the OPPOSITE measurement (`'m'` is capturable). Two halves of
        // one file disagreeing, and the false half was the one reaching operators:
        // CONCURRENTLY is the standard production refresh (it does not lock readers),
        // so the refused config was the common one.
        'm' => RoutingVerdict::Partial(format!(
            "pg cdc: `{cfg}` is a MATERIALIZED VIEW. It CAN be captured — a `REFRESH \
             MATERIALIZED VIEW CONCURRENTLY` decodes as ordinary row events under its \
             own name — but know what that delivers: REFRESH DELTAS, not the base \
             table's own changes, and a PLAIN `REFRESH MATERIALIZED VIEW` emits \
             nothing at all, so a capture whose refresh job uses the plain form sits \
             at 0 rows forever while reporting success. Capture the base table(s) \
             instead if you want the source's changes."
        )),
        // A view and a foreign table have no WAL of their own under any operation.
        'v' | 'f' => {
            let what = match rel.relkind {
                'v' => "a VIEW",
                _ => "a FOREIGN TABLE",
            };
            RoutingVerdict::Never(format!(
                "pg cdc: `{cfg}` is {what}, which writes no WAL under its own name — no change \
                 event can ever carry it, so capture would drop everything while advancing the \
                 slot past it (a success with 0 rows, unrecoverable). Capture the BASE table(s) it \
                 reads instead."
            ))
        }
        // A plain table with inheritance children routes its OWN rows correctly;
        // only rows written directly to a child are dropped. A partial gap — loud
        // enough to see, not fatal enough to refuse a config that works today.
        'r' | 'P' if !rel.children.is_empty() => RoutingVerdict::Partial(format!(
            "pg cdc: `{cfg}` has {} inheritance child table(s) ({}). test_decoding names the CHILD \
             a row landed in, so changes written directly to a child route NOWHERE while the \
             parent's own rows capture normally — a partial, silent gap whose size depends on how \
             the writers address the table. List the children as captured tables if their changes \
             matter.",
            rel.children.len(),
            rel.children.join(", ")
        )),
        _ => RoutingVerdict::Routable,
    }
}

impl PgChangeStream {
    /// Connect and ensure a `test_decoding` logical slot named `slot` exists
    /// (idempotent — reuses an existing slot, which is how a real run resumes).
    ///
    /// `resume_expected` = a prior run's checkpoint exists. In that case a
    /// MISSING slot is a loud error, never a silent re-create: the slot was
    /// dropped or invalidated, and a fresh slot would anchor at the *current*
    /// position — silently skipping every change since the drop.
    ///
    /// A [`DrainMode::BoundedAtOpen`] run snapshots `pg_current_wal_lsn()` once
    /// and stops at the first commit past it — see [`Self::bound`].
    /// Say so when a DELETE from a CAPTURED table will carry only its key.
    ///
    /// `REPLICA IDENTITY` decides what PostgreSQL puts in the OLD tuple. At `FULL` a
    /// delete carries every column; at `DEFAULT` — what a table has unless someone
    /// changed it — it carries the primary key and nothing else. Measured on a real
    /// server at DEFAULT:
    ///
    ///     insert | 1 | 10.5000 | alpha
    ///     update | 1 | 99.9000 | alpha        <- the AFTER image is complete
    ///     delete | 2 |         |              <- key only
    ///
    /// Not corruption and not a reason to refuse: a delete event's job is to say
    /// which key is gone, and `test_decoding` still renders the whole NEW tuple, so
    /// inserts and updates are unaffected. But it is invisible, every test stand sets
    /// FULL so nothing here ever shows it, and it moves any per-row hash computed
    /// over deletes — a CDC prefix and a batch export of the same table then disagree
    /// for a reason nothing in the output explains.
    ///
    /// SCOPED TO THE CAPTURED TABLES. A first cut counted every table in the database
    /// and said "704 table(s)" — true, useless, and the exact shape of a diagnostic
    /// an operator learns to scroll past. A warning about tables nobody is capturing
    /// is noise competing with the one that matters.
    ///
    /// Best-effort: a catalog permission error must not fail a capture that is
    /// otherwise fine.
    pub(crate) fn row_image(
        conn_str: &str,
        tls: Option<&TlsConfig>,
        tables: &[String],
    ) -> crate::source::cdc::RowImage {
        use crate::source::cdc::RowImage;

        if tables.is_empty() {
            return RowImage::Whole;
        }
        // Same CWE-319 gate open() applies (:188): refuse a REMOTE plaintext
        // probe. This best-effort catalog read carries the same credentials, so
        // an ungated remote-plaintext connection here would leak them exactly
        // where open() forbids it (#161). On refusal, fall back to Whole rather
        // than dialing plaintext — open() will bail the run on the same config.
        if require_tls_or_loopback(conn_str, tls).is_err() {
            return RowImage::Whole;
        }
        let Ok(mut client) = (match tls {
            Some(cfg) if cfg.mode.is_enforced() => crate::source::tls::build_native_tls(cfg)
                .and_then(|c| {
                    // Force ssl_mode(Require) so the connector is honored — the
                    // same enforcement the batch path and open() use; without it
                    // this catalog probe carries credentials in cleartext under
                    // an enforced verify-full posture (roast 2026-08-09).
                    super::pg_config_ssl_forced(conn_str)?
                        .connect(postgres_native_tls::MakeTlsConnector::new(c))
                        .map_err(Into::into)
                }),
            _ => Client::connect(conn_str, NoTls).map_err(Into::into),
        }) else {
            return RowImage::Whole;
        };
        // The config may name a table bare or schema-qualified; `relname` is bare.
        let bare: Vec<String> = tables
            .iter()
            .map(|t| t.rsplit('.').next().unwrap_or(t).to_string())
            .collect();
        let Ok(rows) = client.query(
            "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' AND c.relreplident <> 'f' AND c.relname = ANY($1)",
            &[&bare],
        ) else {
            return RowImage::Whole;
        };
        let named: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        Self::row_image_verdict(&named)
    }

    /// Pure verdict half of [`Self::row_image`] (#161, the compression_refusal
    /// split): the captured tables whose REPLICA IDENTITY is not FULL — empty
    /// means every table carries whole rows (keep); any named table downgrades
    /// DELETEs to key-only (warn). Unit-tested in both directions; the
    /// connect+query half stays live-guarded.
    pub(crate) fn row_image_verdict(named: &[String]) -> crate::source::cdc::RowImage {
        use crate::source::cdc::RowImage;
        if named.is_empty() {
            return RowImage::Whole;
        }
        RowImage::KeyOnlyDeletes {
            why: format!(
                "{} of the captured table(s) ({}) have REPLICA IDENTITY other than FULL, so a \
                 DELETE carries ONLY the primary key while INSERT and UPDATE keep their whole \
                 new-row image. `ALTER TABLE <t> REPLICA IDENTITY FULL` if the before-image \
                 matters",
                named.len(),
                named.join(", ")
            ),
        }
    }

    /// [`Self::check_configured_tables_are_routable`] on its own connection, for the
    /// caller to run BEFORE the stream is opened.
    ///
    /// `create_change_stream` wraps the open call in `PG_CDC_HINT` (wal_level and
    /// the REPLICATION attribute), so a routing refusal raised inside `open` reaches
    /// the operator prefixed with "if this is a permissions/setup error" — for a
    /// CONFIG problem with nothing to do with permissions. MEASURED on this branch,
    /// and the same defect the MySQL side was just fixed for, which is why both now
    /// run their check outside the wrap.
    ///
    /// A connect failure is swallowed on purpose: `open` is about to dial the same
    /// server, and its error — WITH the hint — is the right one for that case.
    pub(crate) fn precheck_configured_tables(
        conn_str: &str,
        tls: Option<&TlsConfig>,
        configured: &[String],
    ) -> Result<()> {
        if configured.is_empty() {
            return Ok(());
        }
        if require_tls_or_loopback(conn_str, tls).is_err() {
            return Ok(()); // open() bails on the same posture, with the right message
        }
        let Ok(mut client) = (match tls {
            Some(cfg) if cfg.mode.is_enforced() => crate::source::tls::build_native_tls(cfg)
                .and_then(|c| {
                    super::pg_config_ssl_forced(conn_str)?
                        .connect(postgres_native_tls::MakeTlsConnector::new(c))
                        .map_err(Into::into)
                }),
            _ => Client::connect(conn_str, NoTls).map_err(Into::into),
        }) else {
            return Ok(());
        };
        // The PRECHECK says the note: it runs once per run, where `open`'s call
        // can repeat under the sink's re-drain loop.
        Self::check_configured_tables_are_routable(&mut client, configured, true)
    }

    /// Live half of the routing guard: ask the CATALOG what each configured
    /// table actually is, and refuse a capture that could only ever drop
    /// everything. Decisions live in [`classify_routing`]; this is glue.
    ///
    /// Runs on the stream's OWN connection, before the slot is created and long
    /// before any ack — which is the whole point. A guard that fires after the
    /// first roll has already let PostgreSQL free the WAL.
    ///
    /// `to_regclass` (not `::regclass`) so a name that resolves to nothing
    /// returns NULL instead of raising: an absent table is the schema probe's
    /// error to report (`SELECT * FROM {table}`, `cdc/mod.rs`), with a message
    /// that already names it. Resolution follows the same SQL rules that probe
    /// does, so the two agree about which relation a config string means.
    pub(crate) fn check_configured_tables_are_routable(
        client: &mut Client,
        configured: &[String],
        // Whether to LOG the inert-twin note. This function is called twice by
        // design — a precheck on its own connection, then again inside `open` — and
        // the duplicate is justified as "one round-trip on a config that is about to
        // fail anyway". That reasoning holds for a REFUSAL and not for a note on a
        // config that SUCCEEDS: the note then printed twice per run, forever, on
        // every scheduler cycle. A reporting view named after a table is not a
        // condition that resolves.
        say_note: bool,
    ) -> Result<()> {
        use anyhow::Context as _;
        for cfg in configured {
            // RESOLUTION-FIRST — one decision instead of two. The catalog query
            // returns every relation the configured string could mean under EITHER
            // rule that is live today: what PostgreSQL itself would pick
            // (`to_regclass`, which FOLDS case and accepts `db.schema.table`), and
            // what the byte-exact router would capture (`sink::table_matches`: the
            // whole string as a relname — a quoted name may contain dots — or a
            // `schema.table` split on the FIRST dot). The union goes through
            // `identity::resolve_captured_table`, whose ambiguity arm subsumes the
            // bare-name block that used to sit here AND the folded-twin case that
            // `classify_routing`'s first arm catches after the fact: with both
            // `mixedcase` and `"MixedCase"` present, `table: MixedCase` now refuses
            // naming BOTH relations, where the fold arm named only the one the
            // probe resolved.
            let matches: Vec<crate::source::cdc::identity::CatalogMatch> = client
                .query(
                    // Every relkind logical decoding can name, VIEWS included: a view
                    // sharing the name is a CANDIDATE for the ambiguity message, not
                    // something to filter out. Hiding it makes resolution pick the
                    // table silently while classification refuses the view — two
                    // reads of one fact naming two relations (measured on MySQL).
                    "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                            (c.relkind IN ('r','m') AND c.relpersistence = 'p') \
                              AS capturable \
                     FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.relkind IN ('r','p','m','v','f') \
                       AND n.nspname NOT IN ('pg_catalog','information_schema') \
                       AND n.nspname NOT LIKE 'pg_temp%' \
                       AND n.nspname NOT LIKE 'pg_toast%' \
                       AND (c.oid = to_regclass($1) \
                            OR c.relname = $1 \
                            OR n.nspname || '.' || c.relname = $1 \
                            OR (position('.' in $1) > 0 \
                                AND n.nspname = split_part($1, '.', 1) \
                                AND c.relname = substr($1, position('.' in $1) + 1)))",
                    &[&cfg.as_str()],
                )
                .with_context(|| format!("pg cdc: listing catalog matches for `{cfg}`"))?
                .iter()
                .map(|r| crate::source::cdc::identity::CatalogMatch {
                    schema: r.get(0),
                    table: r.get(1),
                    kind: r.get::<_, String>(2),
                    // Which relkinds can actually CARRY an event, measured rather
                    // than assumed. `'p'` is NOT one: a partitioned PARENT stores no
                    // rows, and `test_decoding` names the LEAF — measured, an insert
                    // into `hunt_arch.hunt_pt` decodes as `table
                    // hunt_arch.hunt_pt_p1: INSERT`, which a bare `hunt_pt` never
                    // matches. `classify_routing` says the same thing 200 lines below,
                    // and counting the parent as a competitor hard-refused a healthy
                    // config over a twin that cannot put one row in the export.
                    //
                    // `'m'` IS capturable because `test_decoding` DOES
                    // decode a materialized view — `CREATE MATERIALIZED VIEW … WITH
                    // DATA` renders as ordinary INSERTs, which is how a same-named
                    // matview injected a fabricated row into an export earlier today.
                    // A plain view (`'v'`) and a foreign table (`'f'`) emit nothing,
                    // and neither does an UNLOGGED relation, so those are context
                    // rather than competing identities. TEMP schemas are excluded by
                    // the query itself: a STRANGER's `CREATE TEMP TABLE` of the same
                    // name made capture flap green/red on a config nobody touched.
                    capturable: r.get::<_, bool>(3),
                })
                .collect();
            let mut resolved: Option<(String, String)> = None;
            if !matches.is_empty() {
                // The ambiguity decision. A NO-match stays with the schema probe
                // below for now — it reports the missing relation today and tests
                // assert its message; unifying that report is a later pass, said
                // here rather than silently deferred.
                let elected = crate::source::cdc::identity::resolve_captured_table(cfg, &matches)?;
                if let Some(note) = &elected.note {
                    // Resolution succeeded, but the name also matched something that
                    // emits nothing. Very often that inert twin is what the operator
                    // meant, and silence sends them hunting.
                    if say_note {
                        log::warn!("pg cdc: {note}");
                    }
                }
                // CARRY the election forward. Re-resolving the configured string
                // below through `to_regclass` (i.e. `search_path`) is how one run
                // came to print two contradictory sentences: a warning that `vv`
                // resolves to deep.vv, then an error that `vv` is a VIEW and to
                // "capture the BASE table(s) it reads" — naming the relation the
                // warning had just elected. MySQL got this half in 62a7876 and
                // PostgreSQL did not.
                resolved = Some((elected.schema, elected.table));
            }
            let (rs, rt): (Option<&str>, Option<&str>) = match &resolved {
                Some((sch, tbl)) => (Some(sch.as_str()), Some(tbl.as_str())),
                None => (None, None),
            };
            let Some(row) = client
                .query_opt(
                    "SELECT c.relkind::text, c.relpersistence::text, \
                            n.nspname::text, c.relname::text, \
                            coalesce(array_agg(n2.nspname || '.' || c2.relname) \
                                     FILTER (WHERE c2.oid IS NOT NULL), '{}'), \
                            CASE WHEN c.relkind = 'p' THEN ( \
                                SELECT coalesce(array_agg(pt.relid::regclass::text), '{}') \
                                FROM pg_partition_tree(c.oid) pt WHERE pt.isleaf) \
                            ELSE '{}' END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     LEFT JOIN pg_inherits i ON i.inhparent = c.oid \
                     LEFT JOIN pg_class c2 ON c2.oid = i.inhrelid \
                     LEFT JOIN pg_namespace n2 ON n2.oid = c2.relnamespace \
                     WHERE (($2::text IS NULL AND c.oid = to_regclass($1)) \
                        OR (n.nspname = $2 AND c.relname = $3)) \
                     GROUP BY c.relkind, c.relpersistence, n.nspname, c.relname, c.oid",
                    &[&cfg.as_str(), &rs, &rt],
                )
                .with_context(|| {
                    format!("pg cdc: reading pg_class to check that `{cfg}` is routable")
                })?
            else {
                continue; // unresolvable here — the schema probe reports it, loudly
            };
            let relkind: String = row.get(0);
            let relpersistence: String = row.get(1);
            let resolved_schema: String = row.get(2);
            let resolved_table: String = row.get(3);
            let children: Vec<String> = row.get(4);
            let leaves: Vec<String> = row.get(5);
            let rel = RelationRouting {
                configured: cfg,
                relkind: relkind.chars().next().unwrap_or('?'),
                relpersistence: relpersistence.chars().next().unwrap_or('p'),
                resolved: (resolved_schema, resolved_table),
                leaves,
                children,
            };
            match classify_routing(&rel) {
                RoutingVerdict::Routable => {}
                // Gated exactly like the note above, and for a stronger reason: a
                // `Partial` config SUCCEEDS, so this repeats on every scheduler
                // cycle forever, while a note usually precedes a bail and is seen
                // once anyway. The precheck runs twice per run (preflight, then
                // open).
                RoutingVerdict::Partial(why) => {
                    if say_note {
                        log::warn!("{why}");
                    }
                }
                RoutingVerdict::Never(why) => anyhow::bail!("{why}"),
            }
        }
        Ok(())
    }

    pub(crate) fn open(
        conn_str: &str,
        slot: &str,
        resume_expected: bool,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        configured_tables: &[String],
    ) -> Result<Self> {
        // Same gate the batch path uses: refuse remote plaintext (CWE-319), and
        // use a verifying TLS connector when a TlsConfig is enforced.
        require_tls_or_loopback(conn_str, tls)?;
        let mut client = match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let connector = crate::source::tls::build_native_tls(cfg)?;
                // Force ssl_mode(Require) like the batch path — otherwise
                // tokio-postgres picks TLS from the URL's sslmode and a
                // `?sslmode=disable` (or a `prefer` downgrade) ships the CDC
                // stream in cleartext, ignoring the connector we just built
                // (roast 2026-08-09: the batch leg was fixed, the CDC leg was
                // not — the exact posture verify-full is asked to forbid).
                super::pg_config_ssl_forced(conn_str)?
                    .connect(postgres_native_tls::MakeTlsConnector::new(connector))?
            }
            _ => Client::connect(conn_str, NoTls)?,
        };
        // test_decoding renders values as TEXT in the polling SESSION's format, so
        // pin the formats this reader's parser assumes — otherwise a non-default
        // database `datestyle` (e.g. 'German, DMY') nulls every timestamp and a
        // non-hex `bytea_output` corrupts every bytea, silently (verified via the
        // source-parity sweep under a flipped session). Immune to the DB default.
        client.batch_execute(
            // extra_float_digits=3 pins SHORTEST-EXACT float text rendering. On a
            // session where it is <= 0 (pre-PG12 default, or set for dump compat)
            // float8out/float4out ROUND to ~15/~6 sig digits, so the text reader
            // parses a lossy value while the batch binary path stays exact — a
            // silent CDC-vs-batch float divergence (bug hunt 2026-08-09, same
            // session-state-rendering class as datestyle/bytea/intervalstyle).
            "SET datestyle = 'ISO, MDY'; SET bytea_output = 'hex'; \
             SET intervalstyle = 'postgres'; SET extra_float_digits = 3;",
        )?;
        // Also here, not only in the caller's precheck. The precheck exists so the
        // refusal reaches the operator WITHOUT the wal_level/REPLICATION hint
        // wrapped around it; this one keeps the guarantee for any path that opens a
        // stream directly. The duplicate catalog read costs one round-trip on a
        // config that is about to fail anyway.
        // The refusal still runs here; the NOTE was already said by the precheck.
        Self::check_configured_tables_are_routable(&mut client, configured_tables, false)?;

        // A bounded run cannot work on a STANDBY: it pins its ceiling with
        // pg_current_wal_lsn() (unavailable during recovery) and a fresh run
        // creates the logical slot (also refused in recovery). Detect recovery
        // up front so the error names the fix, not whichever operation happens
        // to fail first (slot-create vs wal_lsn).
        if mode.is_bounded() {
            let in_recovery: bool = client.query_one("SELECT pg_is_in_recovery()", &[])?.get(0);
            if in_recovery {
                anyhow::bail!(
                    "bounded (until_current) CDC cannot run on a PostgreSQL standby — it is in \
                     recovery, where pg_current_wal_lsn() is unavailable and a logical slot cannot \
                     be created. Stream continuously (until_current: false) or point the source at \
                     the primary."
                );
            }
        }
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )?
            .get(0);
        if !exists {
            if resume_expected {
                anyhow::bail!(
                    "pg cdc: slot '{slot}' is missing but a resume checkpoint exists — the slot \
                     was dropped or invalidated, and the changes since then are no longer in the \
                     log. Re-snapshot the table (mode: full) and restart CDC from a fresh \
                     checkpoint (delete the checkpoint file to accept a new slot)."
                );
            }
            // Creating the slot anchors capture at the CURRENT WAL position:
            // everything already written is unreachable from here. That is correct
            // and expected on a first run — and indistinguishable, from inside this
            // process, from a slot an admin or a failover dropped out from under a
            // running deployment.
            //
            // The hard bail above only fires when a checkpoint FILE proves a prior
            // run, and `cdc.checkpoint` is optional on PostgreSQL precisely because
            // the slot itself is the server-side anchor. So the configuration most
            // reliant on the slot is the one with no evidence that it ever existed,
            // and the silent branch was the one it took. Loud beats silent: rivet
            // cannot know which case this is, but the operator can.
            log::warn!("{}", slot_created_warning(slot));
            client.execute(
                "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&slot],
            )?;
        }
        // Snapshot the bound AFTER the slot exists, so a commit landing between
        // slot creation and this read is ≤ bound (captured this run, not lost
        // between the anchor and the ceiling). A malformed rendering falls back
        // to unbounded — pure catch-up — never an early exit.
        let bound = if mode.is_bounded() {
            use anyhow::Context as _;
            let lsn: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .context(
                    "bounded (until_current) CDC pins its ceiling with pg_current_wal_lsn(), \
                     which is unavailable during recovery — on a standby, stream continuously \
                     (until_current: false) or point the source at the primary",
                )?
                .get(0);
            parse_lsn(&lsn)
        } else {
            None
        };
        Ok(Self {
            client,
            slot: slot.to_string(),
            pending: VecDeque::new(),
            batch_limit: wire_budget(peek),
            frontier: 0,
            exhausted: false,
            pending_truncate_refusal: None,
            bound,
            yielded_data: false,
            frontier_text: None,
            configured_tables: configured_tables.to_vec(),
        })
    }

    /// Peek **one bounded batch** into `pending` **without consuming it**
    /// (`pg_logical_slot_peek_changes(slot, NULL, batch_limit)`). `upto_nchanges`
    /// caps the batch at a **commit boundary** (PostgreSQL only stops after a
    /// whole transaction), so memory is O(batch), never O(total backlog). The
    /// slot is advanced later, in [`ChangeStream::ack`], once the changes are
    /// durably written — a crash before durability re-reads them (at-least-once).
    ///
    /// Refill safety: a peek always starts at the slot's `restart_lsn`, which the
    /// consumer's ack advances between batches. Until that ack lands the same
    /// changes are visible again, so this drops any transaction whose COMMIT LSN
    /// is at or below [`Self::frontier`] (already yielded). A peek that adds no
    /// new transaction — or returns less than a full batch — has drained
    /// everything past the ack frontier and marks the stream [`Self::exhausted`].
    fn fill(&mut self) -> Result<()> {
        // The refusal deferred by the previous window. By now the sink has flushed,
        // checkpointed and acked everything that preceded the truncate, so the slot
        // sits at the last commit before it and the remedy is lossless for the rest.
        if let Some(why) = self.pending_truncate_refusal.take() {
            anyhow::bail!(why);
        }
        let rows = self.client.query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_changes($1, NULL, $2)",
            &[&self.slot, &self.batch_limit],
        )?;
        let n_rows = rows.len();
        // Frame transactions (BEGIN … changes … COMMIT) and stamp every change with
        // its transaction's COMMIT LSN — the only valid slot-advance boundary and
        // the commit-boundary resume position. Logical decoding only ever emits
        // complete, committed transactions.
        let mut tx: Vec<ChangeEvent> = Vec::new();
        // Round-2 audit #9: running byte footprint of the buffered transaction —
        // the row cap alone is a poor bound when cells are large. Reset at BEGIN
        // (the start of accumulation), summed on each push.
        let mut tx_bytes = 0usize;
        let mut yielded_any = false;
        for r in rows {
            let lsn: String = r.get(0);
            let data: String = r.get(1);
            if data.starts_with("COMMIT") {
                let commit_lsn = parse_lsn(&lsn).unwrap_or(0);
                match tx_disposition(commit_lsn, self.frontier, self.bound) {
                    TxDisposition::Yield => {
                        if !tx.is_empty() {
                            self.yielded_data = true;
                        }
                        let commit = Position(json!({ "lsn": lsn }));
                        // #158: the shared close — commit LSN on all, committed on
                        // the last only (BEGIN…COMMIT frames one transaction here).
                        // Otherwise a transaction larger than `rollover` rolls +
                        // acks MID-transaction and a crash before the tail's flush
                        // loses it (the slot advanced past the commit — resume
                        // never re-reads it, an at-least-once break).
                        let mut group: Vec<ChangeEvent> = std::mem::take(&mut tx);
                        crate::source::cdc::TxnFramer::close_group(&mut group, &commit);
                        for ev in group {
                            self.pending.push_back(ev);
                        }
                        self.frontier = commit_lsn;
                        self.frontier_text = Some(lsn.clone());
                        yielded_any = true;
                    }
                    // Already yielded on a prior (un-acked) peek ⇒ drop, idempotent.
                    TxDisposition::AlreadyYielded => tx.clear(),
                    // Committed after this bounded run opened — the next run's
                    // work. Peeks return transactions in commit order, so
                    // everything after this one is past the bound too: stop.
                    TxDisposition::PastBound => {
                        tx.clear();
                        self.exhausted = true;
                        break;
                    }
                }
            } else if data.starts_with("BEGIN") {
                tx.clear();
                tx_bytes = 0;
            } else if let Some((schema, table)) = truncate_targets(&data)
                .into_iter()
                .find(|(sc, tb)| truncate_is_ours(sc, tb, &self.configured_tables))
            {
                // A TRUNCATE carries no rows, so the parser below has nothing to
                // build and dropped it silently — leaving the destination holding
                // rows the source no longer has, with no DELETE to retract them
                // and no later capture able to reconcile it.
                //
                // ONE line can name MANY relations (`TRUNCATE a, b`, and every
                // CASCADE that pulls in referencing tables), so this searches the
                // whole list for one of OURS rather than testing the first. A
                // truncate naming only other tables falls through — the routing
                // filter would drop their rows anyway, and failing on them would
                // make one truncated table an outage for every export on this
                // server (the MySQL undecodable-rows guard's measured lesson).
                // WHEN to raise it depends on whether anything is at stake.
                //
                // Nothing yielded in this window yet ⇒ there is nothing to flush and
                // nothing an ack could save, so raise NOW: deferring would leave a
                // BOUNDED run (`until_current`) free to reach its ceiling and
                // terminate without ever calling `fill` again — reporting SUCCESS over
                // a truncate, which is worse than the immediate bail. (Measured: the
                // pre-existing single-table truncate test went GREEN against the first
                // cut of this change, which is how that hole surfaced.)
                //
                // Something WAS yielded ⇒ those transactions precede the truncate and
                // are still owed to the destination. Bailing now discards them, and the
                // refusal's own remedy — advance the slot past the truncate — then
                // destroys them for good. So end the window instead: the sink flushes,
                // checkpoints and acks that span, and `fill` raises on the next pass
                // with the slot sitting exactly at the last commit before the truncate.
                let why = truncate_refusal_message(&schema, &table);
                // `yielded_any` ALONE. The first cut also required `tx.is_empty()`,
                // and that wedged the capture permanently: events sit in `tx` until
                // their COMMIT line moves them to `pending`, and the `break` below
                // means that COMMIT is never reached — so `BEGIN; INSERT; TRUNCATE;
                // COMMIT` deferred the refusal with `pending` empty, the drain ended
                // clean, `fill` was never called again, and the refusal never fired.
                //
                // MEASURED: `status: success, rows: 0`, exit 0, `_SUCCESS` written,
                // zero warnings — and it does not recover. The slot never advances,
                // so that transaction heads EVERY later window: two rows inserted
                // afterwards were invisible across three more runs, all green, with
                // WAL accumulating.
                //
                // Nothing is at stake in the in-flight `tx`: it was never handed to
                // the sink, so bailing discards only uncommitted events the next run
                // re-reads. What must be protected is what was already YIELDED —
                // exactly what `yielded_any` measures.
                if !yielded_any {
                    anyhow::bail!(why);
                }
                self.pending_truncate_refusal = Some(why);
                self.exhausted = true;
                break;
            } else if let Some(ev) = parse_test_decoding(&lsn, &data)? {
                tx_bytes = tx_bytes.saturating_add(ev.estimated_bytes());
                tx.push(ev);
                // Memory backstop, matching the MySQL adapter's MAX_TX_ROWS: a
                // transaction is buffered whole (never split across parts), so an
                // oversized one grows unbounded. `upto_nchanges` cannot split a
                // transaction, so `peek_changes` already materialised the whole
                // thing into `rows` — this bails loudly instead of compounding it
                // into `pending` + the sink buffer, and names the (upstream) fix.
                let cap = crate::source::cdc::max_tx_rows();
                if tx.len() > cap {
                    anyhow::bail!(
                        "pg cdc: a single transaction has more than {cap} rows — \
                         it must be buffered whole (a transaction is never split across parts), \
                         so this would exhaust memory. Split the source transaction, or raise \
                         the cap only if a transaction this large is genuinely expected."
                    );
                }
                // Round-2 audit #9: byte backstop — a few large-cell rows stay
                // under the row cap yet exhaust memory.
                let byte_cap = crate::source::cdc::max_tx_bytes();
                if tx_bytes > byte_cap {
                    anyhow::bail!(
                        "pg cdc: a single transaction buffered more than {byte_cap} bytes \
                         (large cells) before its commit — it must be buffered whole, so this \
                         would exhaust memory. Split the source transaction, or raise \
                         RIVET_CDC_MAX_TX_BYTES only if a transaction this large is expected."
                    );
                }
            }
        }
        // Short window (backlog fit in one peek) OR a full window that yielded
        // nothing new (every transaction in it was already yielded on a prior
        // un-acked peek — the slot is starved because the sink has not yet acked
        // past the consumed span): either way there is nothing more readable
        // from the CURRENT slot position. Mark exhausted and hand control back
        // to the sink. The sink's re-drain loop then flushes + acks the consumed
        // span (`run_to_files`), which advances the slot and clears `exhausted`,
        // so the NEXT peek slides past a foreign/empty span of ANY size — no
        // budget escalation, no premature "caught up" while in-bound data
        // remains (the bug the escalation only partially covered: a foreign or
        // empty span larger than the escalated window still exhausted early).
        if n_rows < self.batch_limit as usize || !yielded_any {
            self.exhausted = true;
        }
        Ok(())
    }

    /// Zero-yield release: called at clean exhaust. A run whose every
    /// frontier-covered transaction was EMPTY (see [`Self::yielded_data`])
    /// advances the slot itself — the sink will never ack (it has nothing to
    /// flush), and a data-free span has nothing to lose. A run that yielded
    /// data leaves acking to the sink (the flush→checkpoint→ack durability
    /// order); its trailing empty span becomes the NEXT run's zero-yield case
    /// and is released then. Failure here only delays WAL release — warn, never
    /// fail an otherwise-clean run.
    fn release_empty_frontier(&mut self) {
        if self.yielded_data {
            return;
        }
        let Some(lsn) = self.frontier_text.take() else {
            return;
        };
        if let Err(e) = self.advance_slot(&lsn) {
            log::warn!("pg cdc: could not release the empty-transaction span at {lsn}: {e:#}");
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to `lsn`, validated to the
    /// pg_lsn charset before interpolation — never trust a value into SQL
    /// unchecked, even the slot's own output. Advancing frees the WAL up to
    /// `lsn`, so the next peek starts THERE: clear `exhausted` so the sink's
    /// re-drain reads the fresh span instead of stopping (the slot moved, there
    /// may now be readable WAL that a prior starved peek could not reach).
    fn advance_slot(&mut self, lsn: &str) -> Result<()> {
        if lsn.is_empty() || !lsn.bytes().all(|b| b.is_ascii_hexdigit() || b == b'/') {
            anyhow::bail!("pg cdc: refusing to advance to a malformed LSN {lsn:?}");
        }
        // The postgres crate can't bind `&str` → `pg_lsn`, so the LSN is inlined.
        self.client.execute(
            &format!("SELECT pg_replication_slot_advance($1, '{lsn}'::pg_lsn)"),
            &[&self.slot],
        )?;
        self.exhausted = false;
        Ok(())
    }
}

/// Where one decoded transaction goes, given its COMMIT LSN — the pure heart of
/// the drain's termination contract (see [`PgChangeStream::bound`]).
#[derive(Debug, PartialEq)]
enum TxDisposition {
    /// New and in-bound — yield it and advance the frontier.
    Yield,
    /// At/below the frontier: an un-acked re-read from a prior peek — drop.
    AlreadyYielded,
    /// Past the open-time bound: the bounded run stops here; the next run's
    /// resume (from the un-advanced slot) picks it up.
    PastBound,
}

/// Bound check FIRST: a commit past the ceiling must stop the run even on the
/// very first peek (frontier still 0). A parse-miss `commit_lsn` of 0 can never
/// test past the bound — a malformed LSN delays termination, never loses data.
fn tx_disposition(commit_lsn: u64, frontier: u64, bound: Option<u64>) -> TxDisposition {
    if bound.is_some_and(|b| commit_lsn > b) {
        TxDisposition::PastBound
    } else if commit_lsn > frontier {
        TxDisposition::Yield
    } else {
        TxDisposition::AlreadyYielded
    }
}

/// Wire budget per peek: the sink's ack cadence (the part rollover), clamped to
/// the `pg_logical_slot_peek_changes` int4 arg. Slot progress past a span larger
/// than one window is the sink re-drain loop's job (ack → slide), not a bigger
/// budget's — so this is a flat 1×, and drain RSS stays O(rollover). Pure — an
/// offline mutation guard for the budget.
fn wire_budget(peek: crate::source::cdc::PeekBound) -> i32 {
    peek.rows_capped().min(i32::MAX as usize) as i32
}

/// Parse a `pg_lsn` rendering `X/Y` (two hex halves of a 64-bit position) into a
/// comparable `u64`. `None` on a malformed value — the frontier check then treats
/// it as `0` (never drops a real transaction on a parse miss).
fn parse_lsn(lsn: &str) -> Option<u64> {
    let (hi, lo) = lsn.split_once('/')?;
    let hi = u32::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u32::from_str_radix(lo.trim(), 16).ok()?;
    Some((u64::from(hi) << 32) | u64::from(lo))
}

impl ChangeStream for PgChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        loop {
            // Refill a bounded batch whenever the buffer drains — the ack (from
            // the sink, after a durable part) has advanced the slot, so the next
            // peek reads fresh changes. `fill` marks `exhausted` once nothing new
            // remains readable from the current slot position.
            while self.pending.is_empty() && !self.exhausted {
                if let Err(e) = self.fill() {
                    return Some(Err(e));
                }
            }
            if !self.pending.is_empty() {
                return self.pending.pop_front().map(Ok);
            }
            // Exhausted with nothing to yield. A pure-empty span (DDL churn: many
            // row-less transactions) yields no events to the sink, so the sink's
            // re-drain loop never acks and would stop here — but the span may be
            // LARGER than one peek window. Release the empty prefix (advance the
            // slot past it, which clears `exhausted`), then LOOP to re-peek the
            // fresh WAL beyond it, walking the WHOLE empty span in one call rather
            // than one window per scheduler run. `release_empty_frontier` is a
            // no-op once any data was yielded (the sink owns acking then) or when
            // there is nothing left to release, and `frontier_text.take()` makes
            // it advance at most once per new window — so a run that cannot
            // advance falls through to `None` and the loop terminates.
            self.release_empty_frontier();
            if self.exhausted {
                // Release did not advance the slot (data was yielded, or the span
                // is fully drained) — genuinely nothing more.
                return None;
            }
        }
    }

    /// Advance the slot's `confirmed_flush_lsn` to the last durably-written change
    /// — only now is it safe to let PostgreSQL free that WAL and skip those changes
    /// on the next peek. Called by the sink after a part commits.
    fn ack(&mut self, position: &Position) -> Result<()> {
        let lsn = position
            .0
            .get("lsn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("pg cdc ack: position missing 'lsn'"))?
            .to_string();
        self.advance_slot(&lsn)
    }
}

/// Undo `quote_ident` on ONE identifier: `"order"` -> `order`, `"a""b"` -> `a"b`,
/// `plain` -> `plain`.
///
/// `test_decoding` prints relation and column names through PostgreSQL's own
/// `quote_ident`, which quotes anything that is not a lowercase-only, non-reserved
/// identifier. rivet consumed the result RAW, and every downstream comparison is
/// byte-exact, so the quotes made the name unmatchable — while `validate_table_ident`
/// refuses a config string containing `"`, so no spelling of the config could match
/// either. The routing filter then dropped 100% of that table's events, and because
/// the commit boundary is recorded BEFORE the filter the slot was acked past them:
/// terminal loss, `status: success`, zero rows.
///
/// It is not an exotic input. `quote_ident` quotes reserved words (`order`, `user`,
/// `desc`) AND mixed case, so every PascalCase table an ORM generates — `public."User"`,
/// the Prisma and EF Core default — arrived unmatchable.
fn unquote_ident(raw: &str) -> String {
    let t = raw.trim();
    match t.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        // Inside a quoted identifier PostgreSQL doubles an embedded quote.
        Some(inner) => inner.replace("\"\"", "\""),
        _ => t.to_string(),
    }
}

/// Split `schema.table` where EITHER part may be quoted and a quoted part may itself
/// contain a dot (`"my.schema".t`). A plain `split_once('.')` cuts inside the quotes and
/// yields two identifiers that never existed.
fn split_qualified_ident(qual: &str) -> (String, String) {
    // Walked by ITERATOR, not by a hand-rolled index. The index version was
    // correct, and every mutation of its arithmetic either hung the process
    // (`i += 1` -> `i -= 1` or `*= 1` never advances) or was equivalent — so the
    // gate could neither kill those mutants nor learn anything from them. An
    // advance the code does not write is an advance nothing can break.
    let mut in_quotes = false;
    let mut escaped = false;
    let mut split_at = None;
    for (i, b) in qual.bytes().enumerate() {
        if escaped {
            // The second byte of a doubled quote: consumed as content, and it
            // cannot open or close anything.
            escaped = false;
            continue;
        }
        match b {
            b'"' if in_quotes && qual.as_bytes().get(i + 1) == Some(&b'"') => {
                // A doubled quote inside a quoted identifier is an escaped quote,
                // not the end of it.
                escaped = true;
            }
            b'"' => in_quotes = !in_quotes,
            b'.' if !in_quotes => {
                split_at = Some(i);
                break;
            }
            _ => {}
        }
    }
    match split_at {
        Some(i) => (unquote_ident(&qual[..i]), unquote_ident(&qual[i + 1..])),
        None => (String::new(), unquote_ident(qual)),
    }
}

/// The relation a `test_decoding` TRUNCATE line names, or `None` if the line is
/// not a TRUNCATE.
///
/// `TRUNCATE` is decoded (PostgreSQL 11+) and rendered as
/// `table public.t: TRUNCATE: (no-flags)`, but it carries NO rows — so
/// [`parse_test_decoding`], which builds a change out of the column list, has
/// nothing to return and dropped the line into its catch-all `None`. Measured
/// 2026-08-24 on the pg14 stand: 2 inserts then a TRUNCATE left the source table
/// EMPTY while the run reported `status: success, rows: 2` and wrote both inserts
/// to the destination. Not one line about it at `RUST_LOG=trace` either — zero
/// occurrences of the word.
///
/// That divergence is permanent. The rows left the source with no DELETE events
/// to carry them, so no later capture can reconcile the destination back: the
/// stream is now describing a table state that never existed.
pub(crate) fn truncate_targets(data: &str) -> Vec<(String, String)> {
    // The op keyword sits AFTER the (possibly multi-relation) name list, so find the
    // LAST top-level `": "` rather than the first: a quoted identifier may legally
    // contain `": "` and splitting at the first occurrence cuts a name in half.
    let Some(rest) = data.strip_prefix("table ") else {
        return Vec::new();
    };
    // Anchor on the OPERATION marker, not on a bare `": "`. The line holds two of
    // those — one after the name list, one after the keyword
    // (`table a, b: TRUNCATE: (no-flags)`) — so "the last `": "`" lands after
    // TRUNCATE and yields an empty match. Caught by re-measuring the fix rather
    // than by re-reading it.
    const MARK: &str = ": TRUNCATE:";
    let Some(sep) = find_outside_quotes(rest, MARK) else {
        return Vec::new();
    };
    let quals = &rest[..sep];
    // ONE line can name MANY relations — `TRUNCATE a, b` decodes as
    // `table public.a, public.b: TRUNCATE: (no-flags)`, and every CASCADE that
    // pulls in referencing tables does the same. Treating that as a single name
    // made the guard miss the whole statement: MEASURED with `public.tmulti_a`
    // configured, `TRUNCATE tmulti_a, tmulti_b` left the source EMPTY while the
    // run reported `status: success, rows: 1`. Found by an adversarial pass over
    // the first version of this guard.
    split_top_level_commas(quals)
        .into_iter()
        .map(|q| split_qualified_ident(q.trim()))
        .collect()
}

/// Split a relation LIST on top-level commas — a comma inside a quoted identifier
/// (`"odd,name"`) is part of the name, not a separator.
fn split_top_level_commas(list: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut start = 0usize;
    let b = list.as_bytes();
    for i in 0..b.len() {
        if escaped {
            escaped = false;
            continue;
        }
        match b[i] {
            b'"' if in_quotes && b.get(i + 1) == Some(&b'"') => escaped = true,
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => {
                out.push(&list[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&list[start..]);
    out
}

/// Refuse a TRUNCATE on a captured table, naming why re-running cannot fix it.
///
/// Loud, not a warning: this is an UNRECOVERABLE divergence, and the repo's rule
/// is that those fail rather than degrade. A warning would leave a destination
/// that silently disagrees with the source forever, which is the outcome the
/// `unknown -> default` shape exists to prevent.
pub(crate) fn truncate_refusal_message(schema: &str, table: &str) -> String {
    let qualified = if schema.is_empty() {
        table.to_string()
    } else {
        format!("{schema}.{table}")
    };
    format!(
        "pg cdc: `{qualified}` was TRUNCATEd, and this reader cannot represent that as a \
         change. Skipping it would leave every row the truncate removed sitting in the \
         destination with no DELETE to retract it — the source empty, the destination \
         not, permanently, because those rows left the source without events and no \
         later capture can reconcile them. \
         \
         AND THIS RUN WILL NOT MOVE ON ITS OWN. The peek is non-consuming, so the \
         slot still sits on this commit: every later run meets it first and clean \
         changes queued BEHIND it are blocked too, while the un-acked slot pins WAL. \
         MEASURED — a truncate followed by an ordinary INSERT leaves the second run \
         failing identically. \
         \
         Re-snapshot the table (`mode: full`) to re-establish the baseline, and then \
         get the stream past this commit: \
         SELECT pg_replication_slot_advance('<slot>', '<lsn past the truncate>'); \
         Re-snapshotting ALONE leaves the capture wedged, because the slot has not \
         moved."
    )
}

/// Is a TRUNCATE this run's problem? Same asymmetry as the MySQL undecodable-rows
/// guard, and for the same measured reason: one server's stream carries every
/// table's changes, so refusing without asking whose relation it is makes one
/// truncated table an outage for exports that never read it.
///
/// Empty `configured` means "capture whatever the stream emits", which makes every
/// truncate ours.
pub(crate) fn truncate_is_ours(schema: &str, table: &str, configured: &[String]) -> bool {
    configured.is_empty()
        || configured
            .iter()
            .any(|c| crate::source::cdc::sink::table_matches(c, schema, table))
}

/// Parse one `test_decoding` line into a canonical change, or `None` for the
/// `BEGIN`/`COMMIT` transaction markers and anything unrecognised. The line shape
/// is `table <schema>.<table>: <OP>: <columns…>`; pre-images / typed before-after
/// are deferred.
pub(crate) fn parse_test_decoding(lsn: &str, data: &str) -> Result<Option<ChangeEvent>> {
    let Some((qual, tail)) = data.strip_prefix("table ").and_then(|s| s.split_once(": ")) else {
        return Ok(None);
    };
    let (schema, table) = split_qualified_ident(qual);
    let op = if tail.starts_with("INSERT") {
        ChangeOp::Insert
    } else if tail.starts_with("UPDATE") {
        ChangeOp::Update
    } else if tail.starts_with("DELETE") {
        ChangeOp::Delete
    } else {
        return Ok(None);
    };
    // After `<OP>: ` comes the `col[type]:value …` list (all columns for
    // INSERT/UPDATE; the key for DELETE).
    let body = tail.split_once(": ").map(|(_, c)| c).unwrap_or("");
    // Finding #42: an UPDATE that changes the PRIMARY KEY renders BOTH
    // sections — `old-key: <cols> new-tuple: <cols>` — and a naive scan
    // glues them into one over-long image (the arity guard then bricks the
    // stream on a perfectly legal operation, permanently, with a misleading
    // "DDL" diagnosis). Split them: old-key → before, new-tuple → after.
    // Split at the TOP-LEVEL ` new-tuple: ` — outside any quoted value. Finding
    // #7: a quote-blind split_once matches the FIRST occurrence, so a text key
    // whose value literally contains ` new-tuple: ` (e.g. `name[text]:'a new-tuple:
    // b'`) is cut mid-value, garbling BOTH the before and after images.
    const SEP: &str = " new-tuple: ";
    let (old_key_part, new_part) = match body.strip_prefix("old-key: ") {
        Some(rest) => match find_outside_quotes(rest, SEP) {
            Some(pos) => (Some(&rest[..pos]), &rest[pos + SEP.len()..]),
            None => (None, rest),
        },
        None => (None, body),
    };
    let mut named = parse_columns(new_part);
    let old_named = old_key_part.map(parse_columns);

    // An UPDATE that leaves an externally-stored TOAST column untouched renders
    // that column as `col[type]:unchanged-toast-datum` in the NEW tuple — the
    // value itself is NOT in the WAL (logical decoding never re-logs an
    // unchanged out-of-line datum). REPLICA IDENTITY FULL puts the real value in
    // the pre-image (`old-key`), so recover it by name; otherwise the value is
    // genuinely unavailable and we must NOT write the literal marker as data
    // (silent corruption — same class as the uuid→null loss caught live on GCS).
    // An unrecoverable unchanged-TOAST column is a refusal — but a DEFERRED one.
    // The slot decodes every table in the database, so bailing here would poison
    // capture of unrelated tables that merely share the slot. Record it as the
    // event's `poison`; the sink raises it ONLY if this event routes to a captured
    // table (uncaptured tables are dropped without ever surfacing it).
    let unrecovered = recover_unchanged_toast(&mut named, old_named.as_deref());
    let poison = (!unrecovered.is_empty()).then(|| {
        format!(
            "pg cdc: {schema}.{table}: column(s) [{}] arrived as an unchanged-TOAST \
             datum with no pre-image value — logical decoding does not re-log an \
             externally stored value that an UPDATE leaves unchanged, so rivet cannot \
             recover it and refuses to write the literal `unchanged-toast-datum` marker \
             as data. Capture the full pre-image so the value is preserved: \
             ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL;",
            unrecovered.join(", ")
        )
    });

    let names: std::sync::Arc<[String]> = named.iter().map(|c| c.name.clone()).collect();
    let cols: Vec<RivetValue> = named.into_iter().map(|c| c.value).collect();
    // The wire text names EVERY column — carry the names for every op, so
    // the sink maps by NAME and the whole positional-corruption class
    // (findings #37/#41/#42) is unrepresentable on PostgreSQL.
    let (before, after, image_names) = match op {
        ChangeOp::Delete => (Some(cols), None, Some(names)),
        // A PK-changing UPDATE carries its old key too; the after-image is
        // the new tuple (its names). The old key rides `before`.
        ChangeOp::Update => (
            old_named.map(|o| o.into_iter().map(|c| c.value).collect()),
            Some(cols),
            Some(names),
        ),
        ChangeOp::Insert => (None, Some(cols), Some(names)),
    };
    Ok(Some(ChangeEvent {
        op,
        schema,
        table,
        before,
        after,
        image_names,
        position: Position(json!({ "lsn": lsn })),
        // Placeholder — `fill` overrides this, marking only the LAST event of the
        // transaction as the commit boundary (so the sink never rolls mid-tx).
        // Default `false` is the safe value: a stray event that bypassed `fill`
        // would not trigger a premature roll.
        committed: false,
        seq: 0, // stamped by TxnSeq as the stream is consumed
        poison,
    }))
}

/// Parse a `test_decoding` column list (`name[type]:value name[type]:value …`)
/// into typed [`RivetValue`]s, in column order. Values are quoted with `''`
/// escaping or unquoted (numbers / `t`/`f` / `null`).
/// One parsed `test_decoding` column: name, typed value, and whether the wire
/// form was the unquoted `unchanged-toast-datum` sentinel. That sentinel is an
/// externally-stored TOAST value an UPDATE left untouched — the NEW-tuple image
/// carries only the marker, never the value (see [`recover_unchanged_toast`]).
struct ParsedColumn {
    name: String,
    value: RivetValue,
    toast_unchanged: bool,
}

fn parse_columns(s: &str) -> Vec<ParsedColumn> {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while !rest.is_empty() {
        let Some(lb) = rest.find('[') else { break };
        let Some(rel) = rest[lb..].find("]:") else {
            break;
        };
        // The column NAME precedes '[' — it is DATA for key-only images
        // (finding #41): a DELETE's key must map by name, not position.
        let name = unquote_ident(&rest[..lb]);
        let typ = &rest[lb + 1..lb + rel];
        let after_colon = &rest[lb + rel + 2..];
        let (val, quoted, consumed) = parse_value(after_colon);
        // The sentinel is ALWAYS unquoted; a genuine text value equal to the
        // marker arrives quoted (`'unchanged-toast-datum'`), so the quoted flag
        // disambiguates — no false positive on real data.
        let toast_unchanged = !quoted && val == "unchanged-toast-datum";
        out.push(ParsedColumn {
            name,
            value: map_pg_value(typ, &val, quoted),
            toast_unchanged,
        });
        rest = after_colon[consumed..].trim_start();
    }
    out
}

/// Substitute each NEW-tuple `unchanged-toast-datum` column with the real value
/// from the pre-image (`before`, present under REPLICA IDENTITY FULL), matched
/// by column NAME. Returns the names of columns still unavailable — the DEFAULT
/// replica-identity case, where the pre-image carries only the key so the value
/// is not in the WAL at all and the caller must fail loud (never fabricate).
/// Pure; unit-tested.
fn recover_unchanged_toast(
    after: &mut [ParsedColumn],
    before: Option<&[ParsedColumn]>,
) -> Vec<String> {
    let mut unrecovered = Vec::new();
    for col in after.iter_mut() {
        if !col.toast_unchanged {
            continue;
        }
        let recovered = before.and_then(|b| {
            b.iter()
                .find(|pre| pre.name == col.name && !pre.toast_unchanged)
                .map(|pre| pre.value.clone())
        });
        match recovered {
            Some(v) => {
                col.value = v;
                col.toast_unchanged = false;
            }
            None => unrecovered.push(col.name.clone()),
        }
    }
    unrecovered
}

/// Parse one value at the start of `s`. Returns `(value, quoted, bytes_consumed)`.
/// Find `needle` in `haystack` at the TOP LEVEL — outside any single-quoted
/// value. test_decoding quotes text as `'…'` and doubles an embedded quote
/// (`''`); the section separator ` new-tuple: ` can appear literally inside such
/// a value, and a quote-blind `split_once` would cut there (finding #7). Mirrors
/// [`parse_value`]'s `''`-aware quote scan. `needle` is ASCII (its lead byte
/// `0x20` can never be a UTF-8 continuation/lead byte), so the byte scan never
/// false-matches inside a multi-byte column name.
fn find_outside_quotes(haystack: &str, needle: &str) -> Option<usize> {
    // TWO quoting worlds, and missing either one is a silent drop.
    //
    // `'` opens a string LITERAL — a value. `"` opens a quoted IDENTIFIER — a name.
    // Both escape by doubling, and each is an ordinary character inside the other.
    // This tracked only literals, so a legally-quoted identifier containing an
    // apostrophe flipped it into "inside a literal" with nothing to close it:
    //
    //     table public.tord, public."o'brien": TRUNCATE: (no-flags)
    //
    // is real `test_decoding` output (measured on a pg16 stand) and the `: TRUNCATE:`
    // marker was never found, so `truncate_targets` returned empty and the line fell
    // through to `parse_test_decoding`, which drops it. MEASURED end to end: the
    // source table held 0 rows after the truncate, and the run reported
    // `status: success, rows: 2` with both rows still at the destination and no
    // delete to retract them — the exact silent divergence the TRUNCATE guard was
    // written to close, walked around by one apostrophe in a NEIGHBOURING relation.
    //
    // The same function splits ` new-tuple: ` out of an UPDATE body, where the harm
    // is worse: the fallback labels the whole `old-key: … new-tuple: …` body as the
    // after-image, `parse_columns` yields duplicate names, and the sink's by-name
    // lookup hits the OLD value first — so a column named `"it's"` under REPLICA
    // IDENTITY FULL records the PRE-image as the current value. MEASURED: source at
    // 'NEWVAL', parquet at 'OLDVAL', status success. Permanent, because the row is
    // never touched again.
    //
    // `split_top_level_commas` alongside already tracked identifier quotes; this one
    // did not, and nothing compared them.
    #[derive(PartialEq)]
    enum In {
        Nothing,
        Literal,
        Ident,
    }
    let (b, nb) = (haystack.as_bytes(), needle.as_bytes());
    let mut i = 0;
    let mut state = In::Nothing;
    while i < b.len() {
        match state {
            In::Literal => {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2; // doubled → an escaped quote INSIDE the literal
                        continue;
                    }
                    state = In::Nothing;
                }
                i += 1;
            }
            In::Ident => {
                if b[i] == b'"' {
                    if b.get(i + 1) == Some(&b'"') {
                        i += 2; // doubled → an escaped quote INSIDE the identifier
                        continue;
                    }
                    state = In::Nothing;
                }
                i += 1;
            }
            In::Nothing => {
                if b[i] == b'\'' {
                    state = In::Literal;
                    i += 1;
                } else if b[i] == b'"' {
                    state = In::Ident;
                    i += 1;
                } else if b[i..].starts_with(nb) {
                    return Some(i);
                } else {
                    i += 1;
                }
            }
        }
    }
    None
}

fn parse_value(s: &str) -> (String, bool, usize) {
    let b = s.as_bytes();
    if b.first() != Some(&b'\'') {
        let end = s.find(' ').unwrap_or(s.len());
        return (s[..end].to_string(), false, end);
    }
    // quoted: copy chars, collapsing `''` → `'`, until the lone closing quote.
    let mut v = String::new();
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                v.push('\'');
                i += 2;
            } else {
                return (v, true, i + 1);
            }
        } else {
            let n = utf8_len(b[i]);
            v.push_str(&s[i..i + n]);
            i += n;
        }
    }
    (v, true, i)
}

fn utf8_len(lead: u8) -> usize {
    match lead {
        b if b < 0x80 => 1,
        b if b >> 5 == 0b110 => 2,
        b if b >> 4 == 0b1110 => 3,
        _ => 4,
    }
}

/// Map a `test_decoding` `(type, value)` to a typed [`RivetValue`]. The column
/// type is explicit in the stream, so timestamp-vs-timestamptz is never guessed
/// (no naive-vs-instant hazard). Decimals carry exact text → `Decimal128`.
fn map_pg_value(typ: &str, val: &str, quoted: bool) -> RivetValue {
    if !quoted && val == "null" {
        return RivetValue::Null;
    }
    // One-dimensional arrays: `text[]` / `integer[]` / … render as the PG
    // array literal (`{a,"with,comma",NULL}`); parse to element values so the
    // sink builds a real List column (batch parity), never the literal text.
    if let Some(inner) = typ.strip_suffix("[]") {
        return parse_pg_array_literal(inner, val).map_or_else(
            || RivetValue::Bytes(val.as_bytes().to_vec()),
            RivetValue::Array,
        );
    }
    let t = typ;
    if t == "integer" || t == "bigint" || t == "smallint" || t == "oid" {
        return val.parse::<i64>().map_or(RivetValue::Null, RivetValue::Int);
    }
    if t.starts_with("numeric") || t.starts_with("decimal") {
        return RivetValue::Bytes(val.as_bytes().to_vec());
    }
    if t == "boolean" {
        return RivetValue::Bool(val == "t" || val == "true");
    }
    if t == "double precision" || t == "real" {
        return val
            .parse::<f64>()
            .map_or(RivetValue::Null, RivetValue::Float);
    }
    if t.starts_with("timestamp") {
        return parse_pg_timestamp(val);
    }
    if t == "time" || t == "time without time zone" {
        // "HH:MM:SS[.ffffff]" → microseconds since midnight (the Time64 column
        // the batch export uses; the text rendering would silently null there).
        return parse_pg_time_micros(val).map_or(RivetValue::Null, RivetValue::TimeMicros);
    }
    if t == "interval" {
        // Canonicalise the text rendering ("1 year 2 mons 3 days") to the SAME
        // ISO 8601 string the batch export emits ("P1Y2M3D") — one canon, so
        // CDC and batch outputs of the same value are byte-identical.
        return parse_pg_interval(val)
            .map(|(months, days, us)| {
                RivetValue::Bytes(
                    crate::source::postgres::arrow_convert::pg_interval_to_iso8601(
                        months, days, us,
                    )
                    .into_bytes(),
                )
            })
            .unwrap_or_else(|| RivetValue::Bytes(val.as_bytes().to_vec()));
    }
    if t == "date" {
        return chrono::NaiveDate::parse_from_str(val, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime);
    }
    if t == "uuid" {
        // test_decoding renders the uuid as 36-char hyphenated text; the sink's
        // FixedSizeBinary(16) column (same as the batch export) needs the raw
        // 16 bytes — the text rendering would silently degrade to NULL there.
        return decode_hex(&val.replace('-', ""))
            .filter(|b| b.len() == 16)
            .map_or(RivetValue::Null, RivetValue::Bytes);
    }
    if t == "bytea" {
        // Rendered as `\x…` hex; a Binary column must carry the raw bytes, not
        // the hex string.
        if let Some(hex) = val.strip_prefix("\\x")
            && let Some(b) = decode_hex(hex)
        {
            return RivetValue::Bytes(b);
        }
        return RivetValue::Bytes(val.as_bytes().to_vec());
    }
    // text / varchar / char / json / … → string bytes.
    RivetValue::Bytes(val.as_bytes().to_vec())
}

/// Parse a PG array literal (`{alpha,"with,comma","he said \"hi\"",NULL}`)
/// into element values, mapped through [`map_pg_value`] with the element type.
/// Quoted elements un-escape `\"` and `\\`; the bare token `NULL` is an inner
/// NULL. `None` for anything that isn't a `{…}` literal (fail open to text).
fn parse_pg_array_literal(inner_type: &str, val: &str) -> Option<Vec<RivetValue>> {
    let body = val.strip_prefix('{')?.strip_suffix('}')?;
    if body.is_empty() {
        return Some(Vec::new());
    }
    let b = body.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i <= b.len() {
        if b.get(i) == Some(&b'"') {
            // Quoted element: copy until the closing quote, un-escaping.
            let mut elem = String::new();
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' && i + 1 < b.len() {
                    // The escaped char may be multi-byte UTF-8 — copy the WHOLE char.
                    // Round-6: `b[i+1] as char` + `i += 2` interpreted one byte as a
                    // codepoint and left `i` mid-char, so the next `body[i..i+n]` slice
                    // panicked on a non-char boundary — a process-abort DoS (release is
                    // panic=abort) on a crafted `test_decoding` quoted array element.
                    let n = utf8_len(b[i + 1]);
                    let end = (i + 1 + n).min(b.len());
                    elem.push_str(&body[i + 1..end]);
                    i = end;
                } else {
                    let n = utf8_len(b[i]);
                    elem.push_str(&body[i..i + n]);
                    i += n;
                }
            }
            i += 1; // closing quote
            out.push(map_pg_value(inner_type, &elem, true));
            if b.get(i) == Some(&b',') {
                i += 1;
            } else {
                break;
            }
        } else if b.get(i) == Some(&b'{') {
            // A top-level `{` where a scalar token is expected is a NESTED
            // (multi-dimensional) array literal, e.g. `{{1,2},{3,4}}`. rivet's
            // List column is one-dimensional and cannot hold it, so return None:
            // the caller preserves the raw literal as text bytes and the sink
            // fails LOUD (batch parity, src/source/postgres/arrow_convert.rs),
            // never flattening it to a bogus flat array of NULLs. A `{` inside a
            // quoted text element is handled by the quoted branch above, so this
            // arm fires only on genuine nesting.
            return None;
        } else {
            let end = body[i..].find(',').map(|p| i + p).unwrap_or(body.len());
            let tok = &body[i..end];
            out.push(if tok == "NULL" {
                RivetValue::Null
            } else {
                map_pg_value(inner_type, tok, false)
            });
            if end == body.len() {
                break;
            }
            i = end + 1;
        }
    }
    Some(out)
}

/// Parse "HH:MM:SS[.ffffff]" into microseconds since midnight.
fn parse_pg_time_micros(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    // Range-validate EVERY field, not just the hour: `m`/`s` were parsed as
    // unbounded i64, so an untrusted `12:9999999999999999:00` overflowed `m * 60`
    // (fuzz-found panic, cdc.rs — a `panic=abort` DoS on the CDC stream). Bounding
    // h∈0..24, m∈0..60, s∈0..60 both rejects a malformed time (→ None, handled by
    // the caller) AND makes the arithmetic below unconditionally overflow-free
    // (max 23*3600+59*60+59 = 86399, ×1e6 = 8.6e10, well within i64).
    if parts.next().is_some()
        || !(0..24).contains(&h)
        || !(0..60).contains(&m)
        || !(0..60).contains(&s)
    {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        // Right-pad to 6 digits: ".5" ⇒ 500000 µs.
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some(((h * 3600 + m * 60 + s) * 1_000_000) + us)
}

/// Parse PostgreSQL's `postgres`-style interval text rendering —
/// `[N year(s)] [N mon(s)] [N day(s)] [±HH:MM:SS[.ffffff]]`, each part
/// optional — into `(months, days, microseconds)`.
fn parse_pg_interval(val: &str) -> Option<(i32, i32, i64)> {
    let (mut months, mut days, mut micros) = (0i32, 0i32, 0i64);
    let mut tokens = val.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.contains(':') {
            // The time tail: ±HH:MM:SS[.ffffff].
            let (sign, rest) = match tok.strip_prefix('-') {
                Some(r) => (-1i64, r),
                None => (1i64, tok),
            };
            let t = parse_pg_time_micros_unbounded(rest)?;
            micros = t.checked_mul(sign)?;
            continue;
        }
        // Checked arithmetic on the untrusted count: `n` is an unbounded i32 from
        // the wire, so `n * 12` (years→months) and the running accumulation could
        // overflow — the same fuzz-found panic class as the time parsers above.
        let n: i32 = tok.parse().ok()?;
        match tokens.next()? {
            u if u.starts_with("year") => months = months.checked_add(n.checked_mul(12)?)?,
            u if u.starts_with("mon") => months = months.checked_add(n)?,
            u if u.starts_with("day") => days = days.checked_add(n)?,
            _ => return None,
        }
    }
    Some((months, days, micros))
}

/// As [`parse_pg_time_micros`] but without the 24h bound — an interval's time
/// component may exceed a day (e.g. "25:00:00").
fn parse_pg_time_micros_unbounded(val: &str) -> Option<i64> {
    let (hms, frac) = match val.split_once('.') {
        Some((h, f)) => (h, f),
        None => (val, ""),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let us: i64 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    // Interval time tails are genuinely unbounded (h may exceed 24: "25:00:00"), so
    // the fields cannot be range-bounded like a time-of-day. Use CHECKED arithmetic
    // instead — a value so large its microseconds overflow i64 cannot be
    // represented, so return None (fail the parse) rather than panic=abort the
    // stream. Same fuzz-found overflow class as parse_pg_time_micros.
    let secs = h
        .checked_mul(3600)?
        .checked_add(m.checked_mul(60)?)?
        .checked_add(s)?;
    secs.checked_mul(1_000_000)?.checked_add(us)
}

/// Decode an even-length hex string to bytes; `None` on any non-hex input.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    // Byte-index slicing (`&s[i..i+2]`) below panics on a non-char-boundary, and
    // under the release `panic=abort` profile that aborts the whole process. The
    // uuid/bytea arms of `map_pg_value` feed this arbitrary `test_decoding` wire
    // text, so a crafted non-ASCII even-byte-length cell (e.g. "€€") would be a
    // process-abort DoS mid-CDC. Hex is ASCII by definition, so any non-ASCII
    // input is non-hex → `None` is the correct answer AND makes the slice safe
    // (mirrors mongo::hex_to_bytes). Length is a BYTE length, which now equals
    // the char count.
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Parse a PostgreSQL timestamp rendering (`YYYY-MM-DD HH:MM:SS[.ffffff][±TZ]`).
/// For `timestamptz` the trailing offset is DATA, not decoration: test_decoding
/// renders the instant in the polling session's zone, so at any non-UTC
/// session the offset is non-zero ('… 12:00:00+09') — convert to the UTC
/// instant. (The old code stripped '+…' and treated the wall-clock as UTC —
/// +9h corruption at a Tokyo session — and failed outright on negative
/// offsets, silently nulling every value at a western session.)
fn parse_pg_timestamp(val: &str) -> RivetValue {
    let v = val.trim_end();
    // tz-aware renderings first: %#z accepts +09 / +09:30 / +0930.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(v, fmt) {
            return RivetValue::DateTime(dt.naive_utc());
        }
    }
    let naive = v.trim_end_matches('Z');
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(naive, fmt) {
            return RivetValue::DateTime(dt);
        }
    }
    RivetValue::Null
}

#[cfg(test)]
mod tests {

    use super::*;

    /// The TRUNCATE recogniser and its addressing, graded offline.
    ///
    /// The multi-relation case is here because the FIRST version of this guard
    /// shipped without it and an adversarial pass measured the consequence: with
    /// `public.tmulti_a` configured, `TRUNCATE tmulti_a, tmulti_b` left the source
    /// EMPTY while the run reported `status: success, rows: 1`. One line names
    /// every relation, and every CASCADE that pulls in referencing tables produces
    /// the same shape.
    #[test]
    fn a_truncate_is_recognised_across_a_relation_list_and_refused_only_for_ours() {
        let one = |d: &str| truncate_targets(d);

        assert_eq!(
            one("table public.orders: TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "orders".to_string())],
            "the exact line test_decoding emits for a single relation (measured on pg14)"
        );
        // MEASURED wire text for `TRUNCATE tmulti_a, tmulti_b`.
        assert_eq!(
            one("table public.a, public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "a".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a multi-relation TRUNCATE must yield EVERY relation — missing the list \
             is how the first version of this guard let the statement through"
        );
        assert_eq!(
            one("table \"My Schema\".\"Odd.Name\": TRUNCATE: (no-flags)"),
            vec![("My Schema".to_string(), "Odd.Name".to_string())],
            "a quoted identifier unquotes like every other wire identity (#279) — a \
             dot INSIDE quotes belongs to the name"
        );
        // A comma inside a quoted name is part of the name, not a list separator.
        assert_eq!(
            one("table public.\"odd,name\": TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "odd,name".to_string())],
            "splitting on a quoted comma would invent two relations that do not exist"
        );
        // A DOUBLED quote is an escaped quote INSIDE the name, not the end of it.
        // Mutation testing found this: five mutants in `split_top_level_commas`'
        // escape branch survived, because every quoted fixture above closes its
        // quote immediately and the escape path was never taken. A name like
        // `od"d` re-opens the quoted span if the doubling is mishandled, and the
        // following comma is then read as part of the name — one relation instead
        // of two, and OUR table silently not among them.
        assert_eq!(
            one("table public.\"od\"\"d\", public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "od\"d".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a doubled quote is an ESCAPE; mishandling it swallows the separator and \
             loses every relation after it — including possibly ours"
        );
        // …and the same escape inside a name that also carries a comma, which is
        // where the two rules interact.
        assert_eq!(
            one("table public.\"a\"\",b\": TRUNCATE: (no-flags)"),
            vec![("public".to_string(), "a\",b".to_string())],
            "escape then comma: still ONE relation"
        );

        // A name ENDING in the escaped quote — `a"` renders as `"a"""`, three quotes
        // in a row. The fixtures above all have text after the doubling, so the
        // lookahead `b.get(i + 1)` was never asked about a position where reading
        // BACKWARD would answer differently: `replace + with -` survived CI
        // (2026-08-25) against every case here. Proven to be the discriminator by
        // exhaustive search over 97 655 strings on the alphabet `",.ax` — the
        // shortest witness is `"",`, and this is its realistic form.
        assert_eq!(
            one("table public.\"a\"\"\", public.b: TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "a\"".to_string()),
                ("public".to_string(), "b".to_string())
            ],
            "a name ending in an escaped quote must still close, or the following \
             comma is read as part of it and every later relation disappears"
        );

        // An APOSTROPHE inside a quoted IDENTIFIER — legal SQL, real wire output.
        // The scanner tracked string literals only, so `"o'brien"` opened a literal
        // that never closed and the `: TRUNCATE:` marker was never found: the line
        // fell through and was dropped. MEASURED end to end on a pg16 stand — source
        // 0 rows after the truncate, run reported `status: success, rows: 2` with
        // both rows still at the destination. One apostrophe in a NEIGHBOURING
        // relation walked around the whole guard.
        assert_eq!(
            one("table public.tord, public.\"o'brien\": TRUNCATE: (no-flags)"),
            vec![
                ("public".to_string(), "tord".to_string()),
                ("public".to_string(), "o'brien".to_string())
            ],
            "a quoted identifier containing an apostrophe must not blind the marker \
             scan — the truncate is then silently dropped and the destination keeps \
             rows the source no longer has"
        );
        // ...and the same character inside a string VALUE must still hide a marker
        // that occurs within it. Both quoting worlds, both directions.
        assert!(
            one("table public.t: INSERT: v[text]:'not a : TRUNCATE: marker'").is_empty(),
            "a marker inside a string literal is data, not an operation"
        );

        assert!(
            one("table public.orders: INSERT: id[integer]:1").is_empty(),
            "an ordinary change must not be mistaken for a truncate"
        );
        assert!(one("COMMIT 123").is_empty(), "markers are not truncates");
        assert!(
            one("table public.t: DELETE: id[integer]:1").is_empty(),
            "DELETE stays a change — the guard is for the op with NO rows"
        );

        let cfg = vec!["public.orders".to_string()];
        assert!(
            truncate_is_ours("public", "orders", &cfg),
            "our own table must fail the run — the divergence it leaves is permanent"
        );
        assert!(
            !truncate_is_ours("public", "audit", &cfg),
            "another table's truncate must NOT fail this run: the slot decodes the \
             whole database, so refusing would make one truncated table an outage \
             for every export on it (the MySQL undecodable-rows lesson, #281)"
        );
        assert!(
            truncate_is_ours("public", "audit", &[]),
            "`rivet cdc` with no --table captures whatever the stream emits"
        );
        assert!(
            truncate_is_ours("otherschema", "orders", &["orders".to_string()]),
            "a bare config name matches any schema, exactly as sink::table_matches \
             routes it — a guard asking a different question protects the wrong set"
        );
        // The whole point of parsing the LIST: ours can be anywhere in it.
        let multi = one("table public.other, public.orders: TRUNCATE: (no-flags)");
        assert!(
            multi.iter().any(|(s, t)| truncate_is_ours(s, t, &cfg)),
            "a truncate naming ours SECOND must still be found: {multi:?}"
        );
        assert!(
            !one("table public.x, public.y: TRUNCATE: (no-flags)")
                .iter()
                .any(|(s, t)| truncate_is_ours(s, t, &cfg)),
            "a list naming only OTHER tables must not fail this run"
        );

        let msg = truncate_refusal_message("public", "orders");
        assert!(
            msg.contains("public.orders") && msg.contains("mode: full"),
            "the refusal must name the table AND the re-snapshot that recovers from \
             it; a bail with no way forward just moves the operator's problem: {msg}"
        );
        assert!(
            truncate_refusal_message("", "orders").contains("`orders`"),
            "a schema-less identity must not render a leading dot"
        );
    }

    #[test]
    fn classify_routing_refuses_only_the_relations_no_event_can_ever_name() {
        let rel = |kind: char, children: &[&str]| RelationRouting {
            configured: "public.t",
            relkind: kind,
            relpersistence: 'p',
            // The catalog agrees with the config here, so these cases exercise the
            // relkind arms rather than the identity check above them.
            resolved: ("public".to_string(), "t".to_string()),
            leaves: children.iter().map(|c| c.to_string()).collect(),
            children: children.iter().map(|c| c.to_string()).collect(),
        };

        // A partitioned parent stores no rows: 100% drop, and the slot advances
        // past it. This is the case measured live (2 rows in, 0 captured,
        // unrecoverable).
        let RoutingVerdict::Never(why) = classify_routing(&rel('p', &["public.t_2026_01"])) else {
            panic!("a partitioned parent can never be routed — it must be refused");
        };
        assert!(
            why.contains("public.t_2026_01"),
            "the refusal must name the partitions, because listing them IS the \
             remediation: {why}"
        );

        // SUB-PARTITIONED: the remediation must name a LEAF, never the intermediate
        // partitioned table. `children` there is `ml_2026`, which stores no rows
        // either and is refused by this same guard on the next run — a dead-end
        // remediation, found by an adversarial pass over the first version.
        let sub = RelationRouting {
            configured: "public.ml",
            relkind: 'p',
            relpersistence: 'p',
            resolved: ("public".to_string(), "ml".to_string()),
            children: vec!["public.ml_2026".to_string()], // intermediate, unroutable
            leaves: vec!["public.ml_2026_01".to_string()], // the only relation in the WAL
        };
        let RoutingVerdict::Never(why) = classify_routing(&sub) else {
            panic!("a sub-partitioned parent is still unroutable");
        };
        assert!(
            why.contains("public.ml_2026_01") && !why.contains("public.ml_2026,"),
            "the remediation must name the LEAF — naming the intermediate sends the \
             operator to a config this same guard refuses next run: {why}"
        );

        // …and with no partitions yet, it is still unroutable — the message just
        // cannot name one. A guard that only fired when children exist would miss
        // the empty-parent config entirely.
        let RoutingVerdict::Never(why) = classify_routing(&rel('p', &[])) else {
            panic!("a partitioned parent with no partitions yet is still unroutable");
        };
        assert!(why.contains("no partitions yet"), "{why}");

        for (kind, word) in [('v', "VIEW"), ('f', "FOREIGN TABLE")] {
            let RoutingVerdict::Never(why) = classify_routing(&rel(kind, &[])) else {
                panic!("{kind} writes no WAL under its own name — it must be refused");
            };
            assert!(why.contains(word), "the refusal must say what it is: {why}");
        }

        // A MATERIALIZED VIEW is NOT in that list, and this assertion is the
        // correction of a measured falsehood rather than a relaxation. This loop
        // held `'m'` until the round-3 bughunt, on a comment that had measured one
        // refresh form and generalised it; the candidate query 200 lines below was
        // meanwhile rewritten on the OPPOSITE measurement, so the file contradicted
        // itself and the false half was the one operators saw. MEASURED, pg16 CDC
        // stand, one `test_decoding` slot: `CREATE … WITH DATA` and `REFRESH …
        // CONCURRENTLY` both decode under the matview's own name; only the plain
        // `REFRESH` is silent. CONCURRENTLY is the standard production refresh, so
        // `Never` refused the common case.
        let RoutingVerdict::Partial(why) = classify_routing(&rel('m', &[])) else {
            panic!(
                "a matview refreshed CONCURRENTLY emits row events under its own \
                 name — refusing it refuses a capture that works"
            );
        };
        assert!(
            why.contains("MATERIALIZED VIEW") && why.to_uppercase().contains("CONCURRENTLY"),
            "and the warning must name the ONE refresh form that decodes, since a \
             capture whose refresh job uses the plain form sits at 0 rows forever \
             while reporting success: {why}"
        );
        assert!(
            why.contains("not the base table") || why.contains("REFRESH DELTAS"),
            "it must also say WHAT is delivered — refresh deltas, not the source \
             table's changes — or an operator reads a working matview capture as \
             source CDC: {why}"
        );

        // A plain table is routable, and stays routable — refusing it would break
        // every working config.
        assert_eq!(classify_routing(&rel('r', &[])), RoutingVerdict::Routable);

        // ── the identity check, which runs BEFORE any relkind arm ──────────────
        //
        // Both cases below were MEASURED by an adversarial pass over the first
        // version of this guard, which read only relkind and passed them through.
        let mismatched = RelationRouting {
            configured: "MixedCase",
            relkind: 'r',
            relpersistence: 'p',
            // What `SELECT * FROM MixedCase` actually resolves to: PostgreSQL folds
            // the unquoted name, so the probe reads THIS relation's columns while
            // events spelled `MixedCase` route nowhere.
            resolved: ("public".to_string(), "mixedcase".to_string()),
            leaves: vec![],
            children: vec![],
        };
        let RoutingVerdict::Never(why) = classify_routing(&mismatched) else {
            panic!(
                "a config whose resolved identity differs from what routing compares \
                 must be refused — measured, it wrote rows under the WRONG table's \
                 schema with the real column absent entirely, exit 0"
            );
        };
        assert!(
            why.contains("public.mixedcase"),
            "the refusal must name the CATALOG's spelling — that spelling is the \
             remediation: {why}"
        );

        // The 3-part `db.schema.table` form: to_regclass accepts it when db is the
        // current database, so the probe resolves while table_matches splits on the
        // FIRST dot and compares `db` against the schema.
        let three_part = RelationRouting {
            configured: "rivet.public.orders",
            relkind: 'r',
            relpersistence: 'p',
            resolved: ("public".to_string(), "orders".to_string()),
            leaves: vec![],
            children: vec![],
        };
        assert!(
            matches!(classify_routing(&three_part), RoutingVerdict::Never(_)),
            "a 3-part name resolves for the probe and never matches the router"
        );

        // UNLOGGED: writes no WAL, so no event can ever exist for it.
        let unlogged = RelationRouting {
            configured: "public.t",
            relkind: 'r',
            relpersistence: 'u',
            resolved: ("public".to_string(), "t".to_string()),
            leaves: vec![],
            children: vec![],
        };
        let RoutingVerdict::Never(why) = classify_routing(&unlogged) else {
            panic!("an UNLOGGED table writes no WAL — capture is a guaranteed zero");
        };
        assert!(
            why.contains("SET LOGGED"),
            "the refusal must name the one-line fix: {why}"
        );
        // …and a plain permanent table with a matching identity stays routable, or
        // the guard would refuse every working config.
        assert_eq!(classify_routing(&rel('r', &[])), RoutingVerdict::Routable);

        // Inheritance children are a PARTIAL gap: the parent's own rows route
        // fine, so this warns rather than refusing.
        let RoutingVerdict::Partial(why) = classify_routing(&rel('r', &["public.t_child"])) else {
            panic!("an inheritance parent routes its OWN rows — warn, never refuse");
        };
        assert!(
            why.contains("public.t_child") && why.contains('1'),
            "the warning must name the children and how many: {why}"
        );
    }

    #[test]
    fn parse_pg_array_backslash_before_multibyte_does_not_panic() {
        // Round-6: a quoted array element with a backslash BEFORE a multi-byte UTF-8
        // char (`\é`, `\🎉`) made the parser advance i by 2 and slice mid-char on the
        // next iteration — a process-abort DoS (release panic=abort) on a crafted
        // test_decoding array. It must decode the whole escaped char, not panic.
        // RED before the utf8-aware backslash handling (the old code panicked here).
        let got = parse_pg_array_literal("text", "{\"a\\éb\"}").expect("must parse, not panic");
        assert_eq!(
            got.len(),
            1,
            "one element decoded from the escaped multibyte"
        );
        // A backslash before an emoji, and a truncated multi-byte escape at the end —
        // none may panic.
        for lit in ["{\"\\🎉\"}", "{\"x\\é\"}", "{\"a\",\"\\é\"}", "{\"z\\"] {
            let _ = parse_pg_array_literal("text", lit);
        }
    }

    // URL form (not key=value) so the require_tls_or_loopback gate recognises
    // 127.0.0.1 as loopback.
    // The `postgres-cdc` instance (cdc profile, :5434) — wal_level=logical.
    const CONN: &str = "postgresql://rivet:rivet@127.0.0.1:5434/rivet";
    const SLOT: &str = "rivet_cdc_test";

    // The until_current termination contract, as a pure matrix: the bound wins
    // over the frontier (stop even on the first peek), the frontier dedups
    // un-acked re-reads, and a parse-miss LSN (0) can never trip the bound —
    // a malformed rendering delays termination, never loses data.
    #[test]
    fn tx_disposition_bound_frontier_matrix() {
        use TxDisposition::*;
        // Unbounded (daemon / anchor-only): pure frontier behaviour.
        assert_eq!(tx_disposition(10, 0, None), Yield);
        assert_eq!(tx_disposition(10, 10, None), AlreadyYielded);
        assert_eq!(tx_disposition(9, 10, None), AlreadyYielded);
        // Bounded: at the bound is IN scope (committed before open), past is not.
        assert_eq!(tx_disposition(10, 0, Some(10)), Yield);
        assert_eq!(tx_disposition(11, 0, Some(10)), PastBound);
        // Bound wins over the frontier — even a would-be re-read stops the run.
        assert_eq!(tx_disposition(11, 11, Some(10)), PastBound);
        // Parse-miss commit (0) never trips the bound — it falls through to the
        // frontier path, same as the unbounded stream.
        assert_eq!(tx_disposition(0, 0, Some(10)), AlreadyYielded);
        assert_eq!(tx_disposition(0, 0, None), AlreadyYielded);
    }

    // The offline mutation guard for the peek-budget contract: the CI mutants
    // gate runs `--lib` only, so without this a clamp/cap mutant survives
    // everything but a live run.
    #[test]
    fn wire_budget_is_the_ack_cadence_clamped_to_int4() {
        use crate::source::cdc::PeekBound;
        assert_eq!(wire_budget(PeekBound::Sized(100_000)), 100_000);
        assert_eq!(wire_budget(PeekBound::Sized(0)), 1); // rows_capped clamps up
        assert_eq!(wire_budget(PeekBound::Unbounded), i32::MAX);
    }

    // Staff class #6 (generative fuzz, stable-toolchain flavour): the parsers
    // that face WIRE TEXT must never panic on arbitrary input — they return
    // Option/skip, loudly or silently, but never bring the stream down. The
    // timestamptz-offset and array-escape bugs were classic fuzz shapes; this
    // keeps a generative net under every future parser edit.

    // Regression: a non-ASCII `uuid`/`bytea` cell must not abort the process.
    // decode_hex byte-slices `&s[i..i+2]`; before the `!s.is_ascii()` guard a
    // non-ASCII even-BYTE-length value ("€€" = 6 bytes) sliced mid-char and
    // panicked → under release `panic=abort` a whole-process DoS mid-CDC, fed
    // straight from untrusted test_decoding wire text. RED without the guard.
    #[test]
    fn map_pg_value_non_ascii_uuid_bytea_never_panics() {
        // decode_hex itself: non-ASCII even byte length → None (not a panic).
        assert_eq!(decode_hex("€€"), None); // 6 bytes, even, non-char-boundary
        assert_eq!(decode_hex("aa"), Some(vec![0xaa]));
        // The two arms that feed it arbitrary wire text.
        let _ = map_pg_value("uuid", "€€€€€€€€€€€€€€€€€€", false); // even byte len
        let _ = map_pg_value("bytea", "\\x€€", false);
        // A malformed uuid degrades to Null, not a crash (existing contract).
        assert!(matches!(
            map_pg_value("uuid", "not-hex", false),
            RivetValue::Null
        ));
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        #[test]
        fn parse_test_decoding_never_panics(s in ".{0,200}") {
            let _ = parse_test_decoding("0/ABC", &s);
        }

        #[test]
        fn map_pg_value_never_panics(
            // The `typ` generator MUST reach the byte-slicing arms (uuid/bytea →
            // decode_hex), else the totality claim has a blind spot: a plain
            // `[a-z ]{1,20}` regex realistically never emits the exact tokens
            // `uuid`/`bytea`, so a non-ASCII `val` that panics `decode_hex`'s
            // char-boundary slice slipped past this guard for months. Mix the
            // random regex with the real PG type-name set so those arms are
            // actually exercised against arbitrary (incl. non-ASCII) `val`.
            typ in proptest::prop_oneof![
                "[a-z ]{1,20}(\\[\\])?",
                proptest::sample::select(vec![
                    "uuid".to_string(), "bytea".to_string(), "date".to_string(),
                    "timestamp".to_string(), "timestamptz".to_string(),
                    "numeric".to_string(), "json".to_string(), "jsonb".to_string(),
                    "int4".to_string(), "int8".to_string(), "bool".to_string(),
                ]),
            ],
            val in ".{0,120}",
            quoted in proptest::prelude::any::<bool>(),
        ) {
            let _ = map_pg_value(&typ, &val, quoted);
        }

        #[test]
        fn pg_timestamp_parse_total_and_offset_correct(
            h in 0u32..24, mi in 0u32..60, sec in 0u32..60,
            off_h in -12i32..=14, junk in ".{0,40}",
        ) {
            // Total on junk:
            let _ = parse_pg_timestamp(&junk);
            // Correct on every well-formed offset rendering:
            let rendered = format!("2024-06-15 {h:02}:{mi:02}:{sec:02}{off_h:+03}");
            if let RivetValue::DateTime(dt) = parse_pg_timestamp(&rendered) {
                let wall = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(h, mi, sec)
                    .unwrap();
                let expect = wall - chrono::Duration::hours(off_h as i64);
                proptest::prop_assert_eq!(dt, expect);
            } else {
                proptest::prop_assert!(false, "well-formed rendering must parse: {}", rendered);
            }
        }

        #[test]
        fn array_literal_roundtrips_arbitrary_text_elements(
            elems in proptest::collection::vec(
                proptest::option::of("[^\u{0}]{0,24}"), 0..6
            )
        ) {
            // Render the PG literal the way test_decoding would (quote +
            // escape every non-NULL element), parse, and require the exact
            // element vector back — inner NULLs included.
            let body: Vec<String> = elems
                .iter()
                .map(|e| match e {
                    None => "NULL".to_string(),
                    Some(t) => format!(
                        "\"{}\"",
                        t.replace('\\', "\\\\").replace('"', "\\\"")
                    ),
                })
                .collect();
            let lit = format!("{{{}}}", body.join(","));
            let parsed = parse_pg_array_literal("text", &lit)
                .expect("a rendered literal always parses");
            proptest::prop_assert_eq!(parsed.len(), elems.len());
            for (p, e) in parsed.iter().zip(&elems) {
                match (p, e) {
                    (RivetValue::Null, None) => {}
                    (RivetValue::Bytes(b), Some(t)) => {
                        proptest::prop_assert_eq!(b.as_slice(), t.as_bytes())
                    }
                    other => proptest::prop_assert!(false, "mismatch: {:?}", other),
                }
            }
        }
    }

    // Finding #42: a PK-changing UPDATE renders `old-key: … new-tuple: …`.
    // The naive scan glued both sections into one over-long after-image and
    // the arity guard then PERMANENTLY bricked the stream on a legal
    // operation. The after-image must be exactly the new tuple; the old key
    // rides `before`.
    #[test]
    fn pk_changing_update_splits_old_key_from_new_tuple() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 \
                    new-tuple: id[integer]:2 v[text]:'a'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(2), RivetValue::Bytes(b"a".to_vec())]),
            "after-image is the NEW tuple only"
        );
        assert_eq!(
            ev.before,
            Some(vec![RivetValue::Int(1)]),
            "the old key rides before"
        );
        // A normal (non-PK) update stays a plain after-image.
        let ev = parse_test_decoding("0/ABC", "table public.t: UPDATE: id[integer]:1 v[text]:'b'")
            .unwrap()
            .unwrap();
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Int(1), RivetValue::Bytes(b"b".to_vec())])
        );
        assert_eq!(ev.before, None);
    }

    // Finding #7: the old-key/new-tuple split must be quote-aware. A text key
    // whose value literally contains ` new-tuple: ` (a legal string) must NOT
    // split the images there — a quote-blind split_once cut mid-value, garbling
    // BOTH before and after. The real separator is the top-level one.
    #[test]
    fn pk_change_split_ignores_new_tuple_literal_inside_a_quoted_key() {
        // The old key's text value contains the section-separator substring.
        let line = "table public.t: UPDATE: old-key: k[text]:'a new-tuple: b' \
                    new-tuple: k[text]:'c' v[integer]:9";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        assert_eq!(
            ev.before,
            Some(vec![RivetValue::Bytes(b"a new-tuple: b".to_vec())]),
            "the old key keeps its FULL text value, separator substring included"
        );
        assert_eq!(
            ev.after,
            Some(vec![RivetValue::Bytes(b"c".to_vec()), RivetValue::Int(9)]),
            "the after-image is the real new tuple only, not cut mid-value"
        );
    }

    // RED for finding #24 (non-UTC session): test_decoding renders timestamptz
    // in the POLLING SESSION's zone — at a Tokyo session '03:00Z' renders as
    // '2024-06-15 12:00:00+09'. The parser stripped the offset and treated the
    // wall-clock as UTC (+9h corruption); a NEGATIVE offset ('-05') was not
    // even stripped, so the parse failed and the value silently became NULL.
    // Every prior test ran the session at UTC, where the offset is always +00
    // and the bug is invisible.
    #[test]
    fn timestamptz_offset_is_data_not_decoration() {
        use chrono::NaiveDate;
        let cases = [
            ("2024-06-15 12:00:00+09", (2024, 6, 15, 3, 0, 0, 0)),
            ("2024-06-14 22:00:00-05", (2024, 6, 15, 3, 0, 0, 0)),
            (
                "2024-06-15 08:30:00.123456+05:30",
                (2024, 6, 15, 3, 0, 0, 123_456),
            ),
            ("2024-06-15 03:00:00+00", (2024, 6, 15, 3, 0, 0, 0)),
        ];
        for (rendered, (y, mo, d, h, mi, s, us)) in cases {
            let expected = NaiveDate::from_ymd_opt(y, mo, d)
                .unwrap()
                .and_hms_micro_opt(h, mi, s, us)
                .unwrap();
            assert_eq!(
                parse_pg_timestamp(rendered),
                RivetValue::DateTime(expected),
                "offset must convert to the UTC instant for {rendered:?}"
            );
        }
    }

    // RED tests for the all-types matrix audit findings: TIME arrived as text
    // (the "timestamp" prefix check does not match "time without time zone"),
    // so the strict Time64 builder silently nulled every value; INTERVAL rode
    // as PostgreSQL's text rendering ("1 year 2 mons 3 days") while the batch
    // export canonicalises to ISO 8601 ("P1Y2M3D") — same value, two spellings,
    // breaking CDC↔batch parity.
    #[test]
    fn time_parses_to_micros_and_interval_canonicalises_to_iso8601() {
        let line = "table public.t: INSERT: \
                    t1[time without time zone]:'14:30:00.123456' \
                    iv1[interval]:'1 year 2 mons 3 days' \
                    iv2[interval]:'-1 years' \
                    iv3[interval]:'00:00:00' \
                    iv4[interval]:'3 days 04:05:06.789'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::TimeMicros((14 * 3600 + 30 * 60) * 1_000_000 + 123456),
            "TIME must parse to microseconds-since-midnight"
        );
        assert_eq!(after[1], RivetValue::Bytes(b"P1Y2M3D".to_vec()));
        assert_eq!(after[2], RivetValue::Bytes(b"P-1Y".to_vec()));
        assert_eq!(after[3], RivetValue::Bytes(b"PT0S".to_vec()));
        assert_eq!(after[4], RivetValue::Bytes(b"P3DT4H5M6.789000S".to_vec()));
    }

    // RED test for the finding (caught live on a GCS export, by eye): a uuid
    // column rode through as its 36-char TEXT rendering, but the sink's
    // FixedSizeBinary(16) builder accepts only exactly-16-byte values and
    // silently degrades everything else to NULL — so 100% of the column was
    // lost while every count/sum check still passed. The parse must produce
    // the same raw 16 bytes the batch path produces. Same class: bytea rides
    // as its `\x…` hex TEXT — a Binary column would store the hex string.
    #[test]
    fn uuid_and_bytea_decode_to_raw_bytes_not_their_text_rendering() {
        let line = "table public.t: INSERT: \
                    u[uuid]:'0b0e0af9-27ec-4c33-b428-a01b27fdd576' \
                    b[bytea]:'\\x48656c6c6f'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        let after = ev.after.unwrap();
        let RivetValue::Bytes(u) = &after[0] else {
            panic!("uuid must be Bytes, got {:?}", after[0]);
        };
        assert_eq!(
            u.len(),
            16,
            "uuid must be the raw 16 bytes, not 36-char text"
        );
        assert_eq!(
            u[..4],
            [0x0b, 0x0e, 0x0a, 0xf9],
            "uuid bytes must match the hyphenated hex"
        );
        assert_eq!(
            after[1],
            RivetValue::Bytes(b"Hello".to_vec()),
            "bytea must decode the \\x hex rendering to raw bytes"
        );
    }

    // Arrays parse to element values (a real List column downstream), never
    // the literal text — including the hostile shapes: commas inside quoted
    // elements, escaped quotes, inner NULLs, and the empty array.
    #[test]
    fn array_literals_parse_to_typed_elements() {
        let line = "table public.t: INSERT: \
                    tags[text[]]:'{alpha,\"with,comma\",\"he said \\\"hi\\\"\",NULL}' \
                    nums[integer[]]:'{1,NULL,3}' \
                    empty[text[]]:'{}'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[0],
            RivetValue::Array(vec![
                RivetValue::Bytes(b"alpha".to_vec()),
                RivetValue::Bytes(b"with,comma".to_vec()),
                RivetValue::Bytes(b"he said \"hi\"".to_vec()),
                RivetValue::Null,
            ])
        );
        assert_eq!(
            after[1],
            RivetValue::Array(vec![
                RivetValue::Int(1),
                RivetValue::Null,
                RivetValue::Int(3),
            ])
        );
        assert_eq!(after[2], RivetValue::Array(Vec::new()));
    }

    // Finding #6 (CDC sibling of the batch #5 multi-dim array fix): a nested /
    // multi-dimensional array literal must NOT flatten into a bogus flat array
    // of NULLs — rivet's List column is one-dimensional and cannot represent it.
    // parse_pg_array_literal returns None on nesting so the caller keeps the raw
    // literal as text bytes and the sink fails LOUD (batch parity), while a
    // legitimate 1-D array still parses. RED against the pre-fix flatten:
    // `{{1,2},{3,4}}` used to return Some([Null, Null, Null, Null]).
    #[test]
    fn multidim_array_literal_is_refused_not_flattened_to_bogus_nulls() {
        // Nested integer / text arrays → None (unrepresentable, fail open to text).
        assert_eq!(parse_pg_array_literal("integer", "{{1,2},{3,4}}"), None);
        assert_eq!(parse_pg_array_literal("text", "{{a,b},{c,d}}"), None);
        // A one-dimensional array is unaffected.
        assert_eq!(
            parse_pg_array_literal("integer", "{1,2,3}"),
            Some(vec![
                RivetValue::Int(1),
                RivetValue::Int(2),
                RivetValue::Int(3),
            ])
        );
        // A `{` INSIDE a quoted text element is a literal brace, not nesting.
        assert_eq!(
            parse_pg_array_literal("text", "{\"{not nested}\"}"),
            Some(vec![RivetValue::Bytes(b"{not nested}".to_vec())])
        );
        // The full test_decoding row keeps the raw literal (Bytes), never a
        // flat Array of NULLs, so the sink can fail loud on it.
        let line = "table public.t: INSERT: grid[integer[]]:'{{1,2},{3,4}}'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        assert_eq!(
            ev.after.unwrap()[0],
            RivetValue::Bytes(b"{{1,2},{3,4}}".to_vec()),
            "multi-dim literal preserved as text, not flattened to Array([Null; 4])"
        );
    }

    #[test]
    fn parses_typed_columns_from_test_decoding() {
        let line = "table public.t: INSERT: id[integer]:1 name[text]:'alice o''brien' \
                    amount[numeric]:150.05 ts[timestamp without time zone]:'2026-06-23 11:58:01' \
                    flag[boolean]:t maybe[integer]:null";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        assert_eq!(ev.op, ChangeOp::Insert);
        assert_eq!(ev.table, "t");
        let after = ev.after.unwrap();
        assert_eq!(after[0], RivetValue::Int(1));
        assert_eq!(after[1], RivetValue::Bytes(b"alice o'brien".to_vec())); // '' → '
        assert_eq!(after[2], RivetValue::Bytes(b"150.05".to_vec())); // decimal text
        assert!(matches!(after[3], RivetValue::DateTime(_)));
        assert_eq!(after[4], RivetValue::Bool(true));
        assert_eq!(after[5], RivetValue::Null);
    }

    // RED for the unchanged-TOAST corruption: an UPDATE that leaves an
    // externally-stored TOAST column untouched renders it as the unquoted
    // `unchanged-toast-datum` marker in the new tuple. The old parser wrote that
    // literal string into the column (silent corruption). With REPLICA IDENTITY
    // FULL the real value rides the `old-key` pre-image — recover it by NAME.
    // (Wire format proven live: see `docs`/CLAUDE.md; the pre-image carries the
    // real value under FULL, only the marker under DEFAULT.)
    #[test]
    fn unchanged_toast_recovers_from_full_pre_image() {
        let line = "table public.t: UPDATE: \
                    old-key: id[integer]:1 small[text]:'a' big[text]:'REAL-VALUE' \
                    new-tuple: id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        let after = ev.after.unwrap();
        // The after-image's `big` must be the recovered pre-image value, NOT the
        // literal marker text.
        assert_eq!(after[2], RivetValue::Bytes(b"REAL-VALUE".to_vec()));
        assert_eq!(after[1], RivetValue::Bytes(b"b".to_vec()));
    }

    // The DEFAULT replica-identity case: no pre-image value for the toasted
    // column exists anywhere in the WAL, so the parser must refuse to fabricate
    // the marker as data — but as a DEFERRED `poison`, not an immediate bail. The
    // slot decodes every table in the DB; bailing here would poison capture of
    // unrelated tables sharing the slot (the parallel-CDC contamination that RED'd
    // two live PG CDC tests off one un-captured DEFAULT-identity table). The sink
    // raises the poison only when the event routes to a captured table.
    #[test]
    fn unchanged_toast_without_pre_image_is_deferred_to_poison_not_an_immediate_bail() {
        let line = "table public.t: UPDATE: \
                    id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line)
            .expect("must NOT bail — the refusal is deferred to the sink")
            .expect("the event is still produced (for commit-boundary tracking)");
        let msg = ev
            .poison
            .expect("an unrecoverable TOAST column must set poison");
        assert!(msg.contains("unchanged-TOAST"), "got: {msg}");
        assert!(msg.contains("big"), "must name the column, got: {msg}");
        assert!(
            msg.contains("REPLICA IDENTITY FULL"),
            "must name the upstream fix, got: {msg}"
        );
    }

    // The clean case: a recoverable/absent TOAST column leaves `poison` None, so
    // the sink never raises anything for this event.
    #[test]
    fn recoverable_toast_update_leaves_poison_none() {
        let line = "table public.t: UPDATE: old-key: id[integer]:1 big[text]:'real' \
                    new-tuple: id[integer]:1 small[text]:'b' big[text]:unchanged-toast-datum";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        assert!(
            ev.poison.is_none(),
            "a full pre-image recovers the value → no poison: {:?}",
            ev.poison
        );
    }

    // Fuzz-found (nightly pg_test_decoding): an untrusted time/interval with an
    // out-of-range or huge field overflowed the `* 1_000_000` / `n * 12` arithmetic
    // → `attempt to multiply with overflow` → panic=abort DoS on the CDC stream.
    // Every parser must now return None (reject the value), never panic.
    #[test]
    fn hostile_time_and_interval_text_never_panics_returns_none() {
        // time-of-day: m/s out of 0..60 range is rejected before the multiply.
        assert_eq!(parse_pg_time_micros("12:9999999999999999:00"), None);
        assert_eq!(parse_pg_time_micros("12:00:9999999999999999"), None);
        assert_eq!(parse_pg_time_micros("99:00:00"), None); // hour out of range too
        // A valid time still parses.
        assert_eq!(parse_pg_time_micros("01:02:03"), Some(3_723_000_000));
        // Unbounded interval time tail: huge hours can't be range-bounded, so the
        // checked arithmetic returns None instead of overflowing.
        assert_eq!(
            parse_pg_time_micros_unbounded("9999999999999999:00:00"),
            None
        );
        assert_eq!(
            parse_pg_time_micros_unbounded("25:00:00"),
            Some(90_000_000_000)
        ); // valid >24h
        // Interval count overflow (years→months, and the running accumulation).
        assert_eq!(parse_pg_interval("9999999999 years"), None);
        assert_eq!(parse_pg_interval("2 years 3 mons 4 days"), Some((27, 4, 0)));
        // And the top-level fuzz entry point must not panic on the hostile time.
        let line = "table public.t: INSERT: id[integer]:1 t[time]:12:9999999999999999:00";
        let _ = parse_test_decoding("0/ABC", line); // must not panic
    }

    // A genuine text value that happens to equal the marker is QUOTED on the
    // wire, so it must survive verbatim — the quoted flag disambiguates the
    // sentinel from real data (no false-positive corruption/refusal).
    #[test]
    fn quoted_marker_text_is_real_data_not_the_sentinel() {
        let line = "table public.t: INSERT: id[integer]:1 note[text]:'unchanged-toast-datum'";
        let ev = parse_test_decoding("0/ABC", line).unwrap().unwrap();
        let after = ev.after.unwrap();
        assert_eq!(
            after[1],
            RivetValue::Bytes(b"unchanged-toast-datum".to_vec())
        );
    }

    #[test]
    #[ignore = "live: requires docker compose postgres (wal_level=logical)"]
    fn streams_insert_update_delete() {
        let mut admin = Client::connect(CONN, NoTls).unwrap();
        // Fresh slot so the test owns its watermark.
        admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&SLOT],
            )
            .unwrap();

        // Slot must exist BEFORE the changes for them to be captured. No bound:
        // this test's changes are committed AFTER the open.
        let mut s = PgChangeStream::open(
            CONN,
            SLOT,
            false,
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            DrainMode::Continuous,
            &[], // this test routes nothing — it reads the stream directly
        )
        .unwrap();
        admin
            .batch_execute(
                "DROP TABLE IF EXISTS cdc_unit; CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT)",
            )
            .unwrap();
        admin
            .batch_execute(
                "INSERT INTO cdc_unit VALUES (1, 10); \
                 UPDATE cdc_unit SET v = 20 WHERE id = 1; \
                 DELETE FROM cdc_unit WHERE id = 1",
            )
            .unwrap();

        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            let ev = ev.unwrap();
            if ev.table == "cdc_unit" {
                ops.push(ev.op);
            }
        }

        // cleanup before asserting (slot pins WAL).
        admin
            .execute("SELECT pg_drop_replication_slot($1)", &[&SLOT])
            .ok();

        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "logical slot must decode INSERT, UPDATE, DELETE in commit order"
        );
    }

    /// The class: `test_decoding` prints identifiers through `quote_ident`, rivet
    /// consumed them raw, every downstream compare is byte-exact, and the config
    /// validator refuses `"` — so a quoted table was unmatchable from BOTH sides.
    /// The slot is acked past the dropped events, which makes the loss terminal.
    #[test]
    fn quoted_identifiers_are_unquoted_so_routing_can_match_them() {
        // A reserved word — quote_ident quotes it.
        let ev = parse_test_decoding("0/1", "table app.\"order\": INSERT: id[integer]:1")
            .expect("parses")
            .expect("an event");
        assert_eq!(ev.schema, "app", "schema must be usable as a plain name");
        assert_eq!(
            ev.table, "order",
            "the table name must be the IDENTIFIER, not its quoted rendering — a byte-exact \
             router cannot match `\"order\"`, and no config string can spell it either"
        );

        // Mixed case — the ORM default (`public.\"User\"`), the same trap without a
        // reserved word in sight.
        let ev = parse_test_decoding("0/2", "table public.\"User\": INSERT: id[integer]:7")
            .expect("parses")
            .expect("an event");
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("public", "User"));
    }

    /// A dot INSIDE a quoted part is not a qualifier separator. `split_once('.')` cut
    /// there and produced two identifiers that never existed.
    #[test]
    fn a_dot_inside_a_quoted_identifier_is_not_a_qualifier_split() {
        let ev = parse_test_decoding("0/3", "table \"my.schema\".t: INSERT: id[integer]:1")
            .expect("parses")
            .expect("an event");
        assert_eq!((ev.schema.as_str(), ev.table.as_str()), ("my.schema", "t"));
    }

    /// PostgreSQL doubles an embedded quote inside a quoted identifier.
    #[test]
    fn a_doubled_quote_inside_an_identifier_collapses_to_one() {
        assert_eq!(unquote_ident("\"a\"\"b\""), "a\"b");
        assert_eq!(unquote_ident("plain"), "plain");
        assert_eq!(unquote_ident("\"\""), "");
    }

    /// The COLUMN twin. A key-only DELETE maps its image BY NAME (a positional rescue
    /// needs full arity, which a key-only image never has), so a quoted key column made
    /// every tombstone an all-NULL row — the load's merge then partitions by a NULL pk,
    /// the tombstone never wins, and the deleted row survives in the warehouse forever
    /// while counts and sums reconcile.
    #[test]
    fn a_quoted_column_name_is_unquoted_so_a_key_only_delete_maps_by_name() {
        let ev = parse_test_decoding("0/4", "table app.t: DELETE: \"userId\"[integer]:7")
            .expect("parses")
            .expect("an event");
        assert_eq!(
            ev.image_names.as_deref(),
            Some(&["userId".to_string()][..]),
            "the key column must carry its identifier, not its quoted rendering"
        );
    }

    /// The byte walk's ESCAPE branch, which the direct `unquote_ident` cases cannot
    /// reach: a doubled quote must not be read as the END of the quoted part, or the
    /// walker leaves quoted state early and the next `.` splits inside an identifier.
    ///
    /// Every one of these goes through `split_qualified_ident`, not the helper — the
    /// mutation gate found the whole loop ungraded because the doubled-quote test
    /// called the helper directly.
    #[test]
    fn the_qualifier_walk_treats_a_doubled_quote_as_an_escape_not_a_terminator() {
        let cases: &[(&str, &str, &str)] = &[
            // an escaped quote in the SCHEMA, then a real separator
            ("\"a\"\"b\".t", "a\"b", "t"),
            // an escaped quote in the TABLE
            ("app.\"c\"\"d\"", "app", "c\"d"),
            // a dot INSIDE a part that also carries an escaped quote — the walker must
            // stay quoted across both
            ("\"a\"\".b\".t", "a\".b", "t"),
            // an escaped quote immediately before the separator
            ("\"x\"\"\".t", "x\"", "t"),
            // no qualifier at all
            ("\"order\"", "", "order"),
        ];
        for (input, schema, table) in cases {
            let ev = parse_test_decoding("0/9", &format!("table {input}: INSERT: id[integer]:1"))
                .expect("parses")
                .expect("an event");
            assert_eq!(
                (ev.schema.as_str(), ev.table.as_str()),
                (*schema, *table),
                "qualifier `{input}` split wrongly — a mis-split yields identifiers that \
                 never existed, and the router then drops every event for the table"
            );
        }
    }
}

#[cfg(test)]
mod slot_creation_warning_tests {
    use super::PgChangeStream;
    use super::slot_created_warning;

    /// A created slot means capture starts at the CURRENT WAL position, so
    /// anything written earlier is unreachable. On a first run that is correct;
    /// on a slot an admin or a failover dropped it is silent data loss, and from
    /// inside the process the two are indistinguishable.
    ///
    /// The hard bail in `open` only fires when a checkpoint FILE proves a prior
    /// run — and `cdc.checkpoint` is optional on PostgreSQL exactly because the
    /// slot is the server-side anchor. So the config most dependent on the slot
    /// had no guard at all and took the silent branch.
    ///
    /// This pins the message because the branch that emits it needs a live
    /// server. It asserts the three things an operator acts on, not the prose.
    #[test]
    fn the_created_slot_warning_names_the_loss_and_the_remedy() {
        let w = slot_created_warning("rivet_orders");
        assert!(w.contains("rivet_orders"), "must name the slot: {w}");
        assert!(
            w.contains("NOT captured"),
            "must say what was skipped, in words a scanning operator catches: {w}"
        );
        assert!(
            w.contains("mode: full"),
            "must name the recovery — re-snapshot — not just the symptom: {w}"
        );
        assert!(
            w.contains("cdc.checkpoint"),
            "must name the setting that upgrades this to a hard error: {w}"
        );
    }

    /// #161: both directions of the replica-identity verdict.
    #[test]
    fn row_image_verdict_both_directions() {
        use crate::source::cdc::RowImage;
        assert!(matches!(
            PgChangeStream::row_image_verdict(&[]),
            RowImage::Whole
        ));
        match PgChangeStream::row_image_verdict(&["orders".into(), "items".into()]) {
            RowImage::KeyOnlyDeletes { why } => {
                assert!(why.contains("2 of the captured table(s)"), "{why}");
                assert!(why.contains("orders, items"), "{why}");
            }
            other => panic!("non-FULL identity must warn, got {other:?}"),
        }
    }
}
