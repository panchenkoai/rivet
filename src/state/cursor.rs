use crate::error::Result;
use crate::types::CursorState;

use super::StateStore;

/// Incremental cursor store — reads and writes `export_state`.
///
/// The cursor records the last extracted value so incremental runs can pick up
/// where the previous run left off.  Invariant I3 (Write Before Cursor) governs
/// the ordering of cursor updates relative to destination writes.
/// Whether a stored cursor's owner is the identity a run progresses on. A legacy owner
/// (attributed from a run's key descriptor, which names the primary column only) also
/// matches a coalesce identity led by that column.
fn identity_matches(owner: &str, expected: &str, legacy: bool) -> bool {
    if owner == expected {
        return true;
    }
    legacy
        && expected
            .strip_prefix("coalesce(")
            .and_then(|rest| rest.strip_prefix(owner))
            .is_some_and(|rest| rest.starts_with(','))
}

/// Whether two recorded streams provably name different objects: an empty one names none, and a bare name may be the qualified one.
pub(super) fn streams_differ(stored: &str, now: &str) -> bool {
    let qualifies = |long: &str, short: &str| {
        long.strip_suffix(short)
            .is_some_and(|q| q.is_empty() || q.ends_with('.'))
    };
    !stored.is_empty() && !now.is_empty() && !qualifies(stored, now) && !qualifies(now, stored)
}

/// Whether two recorded row sets are one: the same text, or a query as written and a substituted query that fills its `${...}` placeholders (what a plan sealed before templates were recorded carries).
fn same_rows(a: &str, b: &str) -> bool {
    a == b || fills(a, b) || fills(b, a)
}

/// Whether `text` holds no placeholder and is `template` with each of its placeholders replaced by some text.
fn fills(template: &str, text: &str) -> bool {
    let around = crate::config::literal_parts;
    let (Some(parts), None) = (around(template), around(text)) else {
        return false;
    };
    let (first, last) = (&parts[0], &parts[parts.len() - 1]);
    let between = text.strip_prefix(first.as_str());
    let Some(between) = between.and_then(|t| t.strip_suffix(last.as_str())) else {
        return false;
    };
    let mut inner = parts[1..parts.len() - 1].iter();
    inner
        .try_fold(between, |rest, part| {
            rest.find(part.as_str()).map(|at| &rest[at + part.len()..])
        })
        .is_some()
}

/// `stream` as the source resolves it: an unqualified name under a one-schema `search_path` is that schema's.
fn resolved(stream: &str, schema: &str) -> String {
    let unresolved = stream.is_empty() || stream.contains('.');
    if unresolved || schema.is_empty() || schema.contains(',') {
        return stream.to_string();
    }
    format!("{schema}.{stream}")
}

/// The stream parts a progress row holds; `None` for a part written before it was recorded.
struct StoredStream {
    stream: Option<String>,
    schema: Option<String>,
    population: Option<String>,
}

impl StoredStream {
    /// What the row was written reading and what `key` reads now, when a recorded part proves them different streams.
    fn another_than(&self, key: &ProgressKey) -> Option<(String, String)> {
        let schema = self.schema.as_deref();
        let now = resolved(&key.stream, &key.schema);
        if let Some(stream) = &self.stream {
            let was = resolved(stream, schema.unwrap_or_default());
            if streams_differ(&was, &now) {
                return Some((was, now));
            }
            let both_unqualified = !stream.contains('.') && !key.stream.contains('.');
            if let Some(schema) = schema.filter(|s| both_unqualified && *s != key.schema) {
                let under = |name: &str, s: &str| {
                    let name = if name.is_empty() { "its query" } else { name };
                    match s {
                        "" => format!("{name} under the server's own search_path"),
                        s => format!("{name} under search_path {s}"),
                    }
                };
                return Some((under(stream, schema), under(&key.stream, &key.schema)));
            }
        }
        let rows = |population: &str| format!("{now} ({population})");
        self.population
            .as_ref()
            .filter(|was| !key.population.is_empty() && !same_rows(was, &key.population))
            .map(|was| (rows(was), rows(&key.population)))
    }
}

/// What the row of a progress key holds: the run it is anchored on with that run's owner, and the progress a run of the key would continue from (worded) with the stream it was written reading.
#[derive(Default)]
struct StoredProgress {
    anchor: Option<(String, String)>,
    held: Option<(String, StoredStream)>,
}

/// What `rivet state accept` found on the row of a progress key.
#[derive(Debug, PartialEq, Eq)]
pub enum Accepted {
    /// The row holds no progress a run of the key would continue from.
    NoProgress,
    /// The progress already belongs to what the key reads.
    Unchanged,
    /// The progress `held` was re-bound from the stream `was` to the stream `now`.
    Rebound {
        held: String,
        was: String,
        now: String,
    },
}

/// The progress a run would continue from, worded for a message: an interrupted run's anchor, else the cursor a clean run seeks from.
fn held_progress(
    cursor: Option<&str>,
    anchor: Option<&str>,
    continues_high_water: bool,
) -> Option<String> {
    match (anchor, cursor) {
        (Some(run), _) => Some(format!("interrupted run {run}")),
        (None, Some(v)) if continues_high_water => Some(format!("cursor `{v}`")),
        _ => None,
    }
}

/// The mode a range-chunk run records as the owner of its anchor (`ExtractionStrategy::mode_label`).
const CHUNKED: &str = "chunked";
/// The owner of an anchor written before owners were recorded: only a keyset run anchored on this row then.
const KEYSET: &str = "keyset";

/// The command that abandons an interrupted run owned by `owner`.
fn abandon_command(owner: &str, export_name: &str) -> String {
    let verb = if owner == CHUNKED {
        "reset-chunks"
    } else {
        "reset"
    };
    format!("rivet state {verb} -c <config> --export {export_name}")
}

/// The settings that checkpoint a run of mode `owner`, as a refusal names them.
fn checkpoint_settings(owner: &str) -> &'static str {
    if owner == CHUNKED {
        "`chunk_checkpoint: true`"
    } else {
        "`chunk_checkpoint: true`, `keyset_incremental: true` or MongoDB's \
         `source.mongo.resume: true`, whichever it had"
    }
}

/// The cursor column an incremental run's `key_descriptor_json` names, or `None` for
/// any other strategy's descriptor.
/// Whose stored progress a run reads and writes: the row it selects and the parts compared before use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressKey {
    /// Row part: the export's name.
    pub(crate) export_name: String,
    /// Row part: the source key (`SourceConfig::state_key`).
    pub(crate) source: String,
    /// Compared part: the relation the query's outermost `FROM` names; empty when it names none.
    pub(crate) stream: String,
    /// Compared part: the `search_path` an unqualified `stream` is resolved in; empty when the source sets none or the name is qualified.
    pub(crate) schema: String,
    /// Compared part: the rows the query selects (`sql::row_set`); empty when the key names none.
    pub(crate) population: String,
    /// Compared part: the cursor column or keyset key; `None` for a strategy that stores no cursor.
    pub(crate) column: Option<String>,
    /// The mode that owns a run this plan leaves interrupted (`ExtractionStrategy::mode_label`).
    pub(crate) mode: &'static str,
    /// Whether a clean run seeks from a committed high-water.
    pub(crate) continues_high_water: bool,
    /// Whether this plan continues an interrupted run of its own mode (`ExtractionStrategy::is_resumable`).
    pub(crate) resumable: bool,
}

impl ProgressKey {
    /// The key of a range-chunk stream with no compared parts, as a fixture outside the crate names one.
    pub fn chunked(export_name: &str, source: &str) -> Self {
        Self {
            export_name: export_name.into(),
            source: source.into(),
            stream: String::new(),
            schema: String::new(),
            population: String::new(),
            column: None,
            mode: CHUNKED,
            continues_high_water: false,
            resumable: true,
        }
    }

    /// The key of a CDC stream: its compared part is the set of tables it captures.
    pub(crate) fn cdc(export_name: &str, source: &str, tables: &[String]) -> Self {
        let mut tables = tables.to_vec();
        tables.sort();
        Self {
            export_name: export_name.into(),
            source: source.into(),
            stream: serde_json::Value::from(tables).to_string(),
            schema: String::new(),
            population: String::new(),
            column: None,
            mode: "cdc",
            continues_high_water: false,
            resumable: false,
        }
    }
}

/// Stored progress as one run may use it; obtained only from [`StateStore::claim`].
pub struct ProgressClaim<'s> {
    state: &'s StateStore,
    key: ProgressKey,
}

impl ProgressClaim<'_> {
    /// The key this claim was granted for.
    pub fn key(&self) -> &ProgressKey {
        &self.key
    }

    /// The stored cursor, refused when it was written for another progress column.
    pub fn cursor(&self) -> Result<CursorState> {
        let column = self
            .key
            .column
            .as_deref()
            .expect("a strategy that needs cursor state has a cursor identity");
        self.state
            .get_owned(&self.key.export_name, &self.key.source, column)
    }

    /// Whether the stored cursor is the high-water key of a page `run_id` committed.
    pub fn cursor_is_a_page_of(&self, run_id: &str) -> Result<bool> {
        match self.cursor()?.last_cursor_value {
            Some(v) => self.state.is_committed_cursor_high(run_id, &v),
            None => Ok(false),
        }
    }

    /// The interrupted run this stream is anchored on, or `None` when no run is in progress.
    pub fn resume_run_id(&self) -> Result<Option<String>> {
        self.state
            .get_resume_run_id(&self.key.export_name, &self.key.source)
    }

    /// The interrupted range-chunk run of this stream and its plan hash; a run left by a rivet that recorded no source is adopted or refused first.
    pub fn chunk_run(&self) -> Result<Option<(String, String)>> {
        let (name, source) = (&self.key.export_name, &self.key.source);
        if let Some(own) = self.state.in_progress_chunk_run(name, Some(source))? {
            return Ok(Some(own));
        }
        let Some((run_id, plan_hash)) = self.state.in_progress_chunk_run(name, None)? else {
            return Ok(None);
        };
        if let Some(other) = self.state.another_source_of(name, source)? {
            crate::rivet_bail!(
                crate::error::codes::STATE_CHUNK_RUN_OWNER_UNKNOWN,
                "export '{name}': chunk checkpoint run '{run_id}' was left in progress by a rivet \
                 that did not record which source it read, and '{name}' holds progress under \
                 more than one source in this state database (`{other}` and `{source}`) — \
                 nothing says whose run it is, so it is not resumed; nothing was read or \
                 written.\n  \
                 Hint: `{}` abandons run '{run_id}'; the next run of each config starts a fresh pass.",
                abandon_command(CHUNKED, name)
            );
        }
        self.state.adopt_chunk_run(&self.key, &run_id)?;
        log::warn!(
            "export '{name}': chunk checkpoint run '{run_id}' predates source tracking — \
             recorded as belonging to `{source}`, which this export reads now"
        );
        Ok(Some((run_id, plan_hash)))
    }

    /// The latest range-chunk run of this stream in any status (one that recorded no source counts), for `reconcile` and `repair`.
    pub fn latest_chunk_run(&self) -> Result<Option<String>> {
        self.state
            .latest_chunk_run_of(&self.key.export_name, &self.key.source)
    }
}

fn descriptor_cursor_column(descriptor: &str) -> Option<String> {
    let d: serde_json::Value = serde_json::from_str(descriptor).ok()?;
    if d.get("strategy").and_then(serde_json::Value::as_str) != Some("incremental") {
        return None;
    }
    d.get("key")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

impl StateStore {
    /// The cursor of `export_name` writing to `scope` (its destination), or the legacy
    /// pre-v30 row no scoped run has claimed yet.
    pub fn get(&self, export_name: &str, scope: &str) -> Result<CursorState> {
        self.adopt_respelled_source(export_name, scope)?;
        Ok(self
            .query_opt(
                "SELECT last_cursor_value, last_run_at, cursor_column FROM export_state \
                 WHERE export_name = ?1 AND (prefix = ?2 OR prefix = '') \
                 ORDER BY prefix DESC LIMIT 1",
                &[export_name.into(), scope.into()],
                |r| CursorState {
                    export_name: export_name.to_string(),
                    last_cursor_value: r.opt_text(0),
                    last_run_at: r.opt_text(1),
                    cursor_column: r.opt_text(2),
                },
            )?
            .unwrap_or_else(|| CursorState {
                export_name: export_name.to_string(),
                last_cursor_value: None,
                last_run_at: None,
                cursor_column: None,
            }))
    }

    /// Move the legacy (unscoped) row to `scope` if this scope has none yet: the first
    /// scoped writer continues it, and no other config sharing the name can read it after.
    fn claim_legacy_row(&self, export_name: &str, scope: &str) -> Result<()> {
        self.adopt_respelled_source(export_name, scope)?;
        if scope.is_empty() {
            return Ok(());
        }
        self.execute(
            "UPDATE export_state SET prefix = ?2 WHERE export_name = ?1 AND prefix = '' \
             AND NOT EXISTS (SELECT 1 FROM export_state WHERE export_name = ?1 AND prefix = ?2)",
            &[export_name.into(), scope.into()],
        )?;
        Ok(())
    }

    /// Move progress stored under another spelling of `scope` (its default port left out, its password unmasked) onto `scope`: the newest such cursor row unless `scope` holds one, and its checkpoint rows; the cursor rows left are deleted.
    pub(super) fn adopt_respelled_source(&self, export_name: &str, scope: &str) -> Result<()> {
        let stored = self.query(
            "SELECT prefix, last_run_at FROM export_state \
             WHERE export_name = ?1 AND prefix NOT IN ('', ?2) \
             UNION SELECT source, NULL FROM chunk_run WHERE export_name = ?1 AND source <> ?2 \
             UNION SELECT source, NULL FROM keyset_range WHERE export_name = ?1 AND source <> ?2 \
             ORDER BY 2 DESC NULLS LAST",
            &[export_name.into(), scope.into()],
            |r| r.text(0),
        )?;
        for respelled in stored
            .iter()
            .filter(|p| crate::config::canonical_source_key(p) == scope)
        {
            for sql in [
                "UPDATE export_state SET prefix = ?2 WHERE export_name = ?1 AND prefix = ?3 \
                 AND NOT EXISTS (SELECT 1 FROM export_state WHERE export_name = ?1 AND prefix = ?2)",
                "UPDATE chunk_run SET source = ?2 WHERE export_name = ?1 AND source = ?3 \
                 AND (status <> 'in_progress' OR NOT EXISTS (SELECT 1 FROM chunk_run c \
                 WHERE c.export_name = ?1 AND c.source = ?2 AND c.status = 'in_progress'))",
                "UPDATE keyset_range SET source = ?2 WHERE export_name = ?1 AND source = ?3 \
                 AND NOT EXISTS (SELECT 1 FROM keyset_range k \
                 WHERE k.export_name = ?1 AND k.source = ?2)",
            ] {
                self.execute(
                    sql,
                    &[export_name.into(), scope.into(), respelled.as_str().into()],
                )?;
            }
            self.execute(
                "DELETE FROM export_state WHERE export_name = ?1 AND prefix = ?2",
                &[export_name.into(), respelled.as_str().into()],
            )?;
        }
        Ok(())
    }

    /// Read the cursor for a run progressing on `expected`, refusing one written for another column.
    fn get_owned(&self, export_name: &str, scope: &str, expected: &str) -> Result<CursorState> {
        let state = self.get(export_name, scope)?;
        let Some(value) = state.last_cursor_value.as_deref() else {
            return Ok(state);
        };
        let (owner, legacy) = match &state.cursor_column {
            Some(c) => (Some(c.clone()), false),
            None => (self.legacy_cursor_owner(export_name, value)?, true),
        };
        if let Some(owner) = owner
            && !identity_matches(&owner, expected, legacy)
        {
            crate::rivet_bail!(
                crate::error::codes::STATE_CURSOR_OWNER_MISMATCH,
                "export '{export_name}': the stored cursor `{value}` was written for `{owner}`, \
                 but this export now progresses on `{expected}` — comparing `{expected}` against \
                 it selects the wrong rows (on MySQL silently none, on every run).\n  \
                 Hint: `rivet state reset -c <config> --export {export_name}` starts `{expected}` over with a \
                 full pass; or restore the previous cursor (`{owner}`)."
            );
        }
        Ok(state)
    }

    /// What the row a claim of `key` selects holds.
    fn stored_progress(&self, key: &ProgressKey) -> Result<StoredProgress> {
        let row = self.query_opt(
            "SELECT stream, last_cursor_value, resume_run_id, resume_owner, source_schema, \
             population FROM export_state \
             WHERE export_name = ?1 AND (prefix = ?2 OR prefix = '') \
             ORDER BY prefix DESC LIMIT 1",
            &[key.export_name.as_str().into(), key.source.as_str().into()],
            |r| {
                (
                    (r.opt_text(0), r.opt_text(4), r.opt_text(5)),
                    (r.opt_text(1), r.opt_text(2), r.opt_text(3)),
                )
            },
        )?;
        let Some(((stream, schema, population), (cursor, run, owner))) = row else {
            return Ok(StoredProgress::default());
        };
        let anchor = run.map(|run| (run, owner.unwrap_or_else(|| KEYSET.to_string())));
        let resumed = anchor.as_ref().filter(|(_, owner)| owner != CHUNKED);
        let held = held_progress(
            cursor.as_deref(),
            resumed.map(|(run, _)| run.as_str()),
            key.continues_high_water,
        );
        let stored = StoredStream {
            stream,
            schema,
            population,
        };
        Ok(StoredProgress {
            anchor,
            held: held.map(|held| (held, stored)),
        })
    }

    /// Claim the stored progress of `key`: refuse progress stored for another stream (relation, schema or rows); adopt a row written before a part was recorded, or under another spelling of the source.
    pub fn claim(&self, key: ProgressKey) -> Result<ProgressClaim<'_>> {
        let (export_name, scope, stream) = (&key.export_name, &key.source, &key.stream);
        self.adopt_respelled_source(export_name, scope)?;
        let StoredProgress { anchor, held } = self.stored_progress(&key)?;
        let anchor = anchor
            .as_ref()
            .map(|(run, owner)| (run.as_str(), owner.as_str()));
        self.refuse_unfinished_run(&key, anchor)?;
        if let Some((run, _)) = anchor {
            self.own_anchor(&key, run)?;
        }
        let Some((held, stored)) = held else {
            return Ok(ProgressClaim { state: self, key });
        };
        if let Some((was, now)) = stored.another_than(&key) {
            crate::rivet_bail!(
                crate::error::codes::STATE_CURSOR_STREAM_MISMATCH,
                "export '{export_name}': its stored progress ({held}) was written reading \
                 `{was}`, but this export reads `{now}` — continuing from it would \
                 skip rows of `{now}`; nothing was read or written.\n  \
                 Hint: two exports sharing a name in one state database need their own names \
                 (or their own state). If this export was edited, restore what it read to \
                 continue from the stored progress, or \
                 `rivet state reset -c <config> --export {export_name}` starts `{now}` over \
                 with a full pass — it discards the progress '{export_name}' holds on this \
                 source. If the edit is meant and the export should go on from that progress, \
                 `rivet state accept -c <config> --export {export_name}` keeps it and records \
                 it as belonging to `{now}` — rows of `{now}` below it are not delivered."
            );
        }
        let unrecorded = [
            ("stream", stored.stream.is_none(), stream),
            ("source_schema", stored.schema.is_none(), &key.schema),
            ("population", stored.population.is_none(), &key.population),
        ];
        for (column, _, now) in unrecorded
            .iter()
            .filter(|(c, legacy, now)| *legacy && (*c == "source_schema" || !now.is_empty()))
        {
            self.execute(
                &format!(
                    "UPDATE export_state SET {column} = ?3 WHERE export_name = ?1 \
                     AND (prefix = ?2 OR prefix = '') AND {column} IS NULL"
                ),
                &[
                    export_name.as_str().into(),
                    scope.as_str().into(),
                    now.as_str().into(),
                ],
            )?;
        }
        if stored.stream.is_none() && !stream.is_empty() {
            log::warn!(
                "export '{export_name}': its stored progress ({held}) predates stream \
                 tracking — recorded as belonging to `{stream}`, which this export reads now"
            );
        } else if stored.population.is_none() && !key.population.is_empty() {
            log::warn!(
                "export '{export_name}': its stored progress ({held}) predates query \
                 tracking — recorded as belonging to `{}`, which this export reads now",
                key.population
            );
        }
        Ok(ProgressClaim { state: self, key })
    }

    /// Refuse `key` while its stream holds an unfinished run this plan does not continue: one of another mode, or of its own mode when the plan keeps no checkpoint.
    fn refuse_unfinished_run(&self, key: &ProgressKey, anchor: Option<(&str, &str)>) -> Result<()> {
        let export_name = &key.export_name;
        let chunk_run = self.unfinished_chunk_run(export_name, &key.source)?;
        if let Some(run) = &chunk_run
            && let Some(later) = self.finished_after_unsourced_chunk_run(export_name, run)?
        {
            crate::rivet_bail!(
                crate::error::codes::STATE_CHUNK_RUN_SUPERSEDED,
                "export '{export_name}': chunk checkpoint run '{run}' was left in progress by \
                 rivet 0.31 or earlier, and run '{later}' of this export finished after it was \
                 opened — the parts '{run}' wrote hold the source as it was before that run, so \
                 it is not resumed; nothing was read or written.\n  \
                 Hint: `{}` abandons run '{run}'; the next run starts a fresh pass.",
                abandon_command(CHUNKED, export_name)
            );
        }
        let unfinished = anchor
            .map(|(run, owner)| (run.to_string(), owner))
            .into_iter()
            .chain(chunk_run.map(|run| (run, CHUNKED)));
        for (run, owner) in unfinished {
            if owner != key.mode {
                crate::rivet_bail!(
                    crate::error::codes::STATE_INTERRUPTED_RUN_OWNER_MISMATCH,
                    "export '{export_name}': run {run} of mode `{owner}` is unfinished, but \
                     this export now runs as `{}` — the progress it stored is that run's, \
                     not a point a `{}` run may continue from; nothing was read or written.\n  \
                     Hint: restore the `{owner}` settings and run once to finish run {run}, \
                     then switch; or `{}` abandons it and the next run starts with a full pass.",
                    key.mode,
                    key.mode,
                    abandon_command(owner, export_name)
                );
            }
            if !key.resumable {
                crate::rivet_bail!(
                    crate::error::codes::STATE_INTERRUPTED_RUN_OWNER_MISMATCH,
                    "export '{export_name}': run {run} of mode `{owner}` is unfinished, but \
                     this export now runs without the checkpoint that run was opened with — a \
                     run that does not continue it would finish beside it, and a later \
                     checkpointed run would resume {run} over data older than that finished \
                     run; nothing was read or written.\n  \
                     Hint: restore the checkpoint setting ({}) and run once to finish run \
                     {run}, then remove it; or `{}` abandons it and the next run starts with a \
                     full pass.",
                    checkpoint_settings(owner),
                    abandon_command(owner, export_name)
                );
            }
        }
        Ok(())
    }

    /// Record `key` as the owner of the anchored run `run_id` on rows written before owners and sources were recorded.
    fn own_anchor(&self, key: &ProgressKey, run_id: &str) -> Result<()> {
        let (name, scope) = (key.export_name.as_str(), key.source.as_str());
        self.execute(
            "UPDATE export_state SET resume_owner = ?3 WHERE export_name = ?1 \
             AND (prefix = ?2 OR prefix = '') AND resume_run_id = ?4 AND resume_owner IS NULL",
            &[name.into(), scope.into(), key.mode.into(), run_id.into()],
        )?;
        self.execute(
            "UPDATE keyset_range SET source = ?2 WHERE export_name = ?1 AND run_id = ?3 \
             AND source IS NULL",
            &[name.into(), scope.into(), run_id.into()],
        )?;
        Ok(())
    }

    /// A source key other than `source` that `export_name` holds progress under, if any.
    pub(super) fn another_source_of(
        &self,
        export_name: &str,
        source: &str,
    ) -> Result<Option<String>> {
        self.query_opt(
            "SELECT prefix FROM export_state WHERE export_name = ?1 AND prefix NOT IN ('', ?2) \
             UNION SELECT source FROM chunk_run WHERE export_name = ?1 AND source <> ?2 \
             UNION SELECT source FROM keyset_range WHERE export_name = ?1 AND source <> ?2 \
             ORDER BY 1 LIMIT 1",
            &[export_name.into(), source.into()],
            |r| r.text(0),
        )
    }

    /// Owner of a pre-v26 row: the key of the latest successful run that wrote this
    /// value — a keyset run's `chunk_key`, or the cursor column an incremental run
    /// named in its key descriptor.
    fn legacy_cursor_owner(&self, export_name: &str, value: &str) -> Result<Option<String>> {
        let latest = self.query_opt(
            "SELECT mode, chunk_key, cursor_max, key_descriptor_json FROM export_metrics \
             WHERE export_name = ?1 AND status = 'success' ORDER BY id DESC LIMIT 1",
            &[export_name.into()],
            |r| (r.opt_text(0), r.opt_text(1), r.opt_text(2), r.opt_text(3)),
        )?;
        Ok(match latest {
            Some((_, _, Some(max), _)) if max != value => None,
            Some((Some(mode), Some(key), Some(_), _)) if mode == "keyset" => Some(key),
            Some((_, _, Some(_), Some(descriptor))) => descriptor_cursor_column(&descriptor),
            _ => None,
        })
    }

    /// Advance the cursor of `key` and record which column/key and which stream it belongs to.
    pub fn update_with_column(&self, key: &ProgressKey, cursor_value: &str) -> Result<()> {
        let (export_name, scope) = (key.export_name.as_str(), key.source.as_str());
        let cursor_column = key.column.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "export '{export_name}': the run committed cursor `{cursor_value}` but its strategy has \
                 no cursor identity to record it under — a defect in the strategy, not the \
                 data; nothing was written"
            )
        })?;
        self.claim_legacy_row(export_name, scope)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state \
             (export_name, prefix, last_cursor_value, last_run_at, cursor_column, stream, \
              source_schema, population)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(export_name, prefix) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at,
                cursor_column = excluded.cursor_column,
                stream = excluded.stream,
                source_schema = excluded.source_schema,
                population = excluded.population";
        self.execute(
            sql,
            &[
                export_name.into(),
                scope.into(),
                cursor_value.into(),
                now.into(),
                cursor_column.into(),
                key.stream.as_str().into(),
                key.schema.as_str().into(),
                key.population.as_str().into(),
            ],
        )?;
        Ok(())
    }

    /// Advance the cursor value without an identity: a pre-v26 row, as a fixture writes
    /// it. Every runner records which column the value belongs to (`update_with_column`).
    #[allow(dead_code)] // fixtures only: unit, offline and live tests stage pre-v26 rows
    pub fn update_legacy(&self, export_name: &str, cursor_value: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state (export_name, last_cursor_value, last_run_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(export_name, prefix) DO UPDATE SET
                last_cursor_value = excluded.last_cursor_value,
                last_run_at = excluded.last_run_at";
        self.execute(sql, &[export_name.into(), cursor_value.into(), now.into()])?;
        Ok(())
    }

    /// Round-5 (keyset checkpoint-resume manifest completeness): persist the
    /// in-progress keyset run_id beside the resume cursor, so a crash+resume reuses
    /// it and reconstructs every committed page's manifest part from file_log. Set on
    /// the first checkpointed run, read on resume, cleared when the run finalizes.
    pub fn set_resume_run_id(&self, key: &ProgressKey, run_id: &str) -> Result<()> {
        let (export_name, scope) = (key.export_name.as_str(), key.source.as_str());
        self.claim_legacy_row(export_name, scope)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_state \
             (export_name, prefix, resume_run_id, last_run_at, stream, resume_owner, \
              source_schema, population)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(export_name, prefix) DO UPDATE SET \
                resume_run_id = excluded.resume_run_id, stream = excluded.stream, \
                resume_owner = excluded.resume_owner, source_schema = excluded.source_schema, \
                population = excluded.population";
        self.execute(
            sql,
            &[
                export_name.into(),
                scope.into(),
                run_id.into(),
                now.into(),
                key.stream.as_str().into(),
                key.mode.into(),
                key.schema.as_str().into(),
                key.population.as_str().into(),
            ],
        )?;
        Ok(())
    }

    /// The persisted in-progress keyset run_id, or None when no run is in progress.
    pub fn get_resume_run_id(&self, export_name: &str, scope: &str) -> Result<Option<String>> {
        self.adopt_respelled_source(export_name, scope)?;
        let sql = "SELECT resume_run_id FROM export_state \
                   WHERE export_name = ?1 AND (prefix = ?2 OR prefix = '') \
                   ORDER BY prefix DESC LIMIT 1";
        Ok(self
            .query_opt(sql, &[export_name.into(), scope.into()], |r| r.opt_text(0))?
            .flatten())
    }

    /// Clear the in-progress run_id once a keyset run has finalized its manifest.
    pub fn clear_resume_run_id(&self, export_name: &str, scope: &str) -> Result<()> {
        self.claim_legacy_row(export_name, scope)?;
        self.execute(
            "UPDATE export_state SET resume_run_id = NULL, resume_owner = NULL \
             WHERE export_name = ?1 AND prefix = ?2",
            &[export_name.into(), scope.into()],
        )?;
        Ok(())
    }

    /// Clear every in-progress run_id this export name holds, in any scope (a split re-cut its windows).
    pub fn clear_resume_run_id_every_scope(&self, export_name: &str) -> Result<()> {
        self.execute(
            "UPDATE export_state SET resume_run_id = NULL, resume_owner = NULL WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        Ok(())
    }

    /// Whether this export name holds an in-progress run_id in any scope.
    pub fn has_resume_run_id_in_any_scope(&self, export_name: &str) -> Result<bool> {
        Ok(self
            .query_opt(
                "SELECT COUNT(*) FROM export_state WHERE export_name = ?1 AND resume_run_id IS NOT NULL",
                &[export_name.into()],
                |r| r.i64(0),
            )?
            .unwrap_or(0)
            > 0)
    }

    /// Null ONLY the persisted keyset high-water mark (`last_cursor_value`),
    /// leaving `resume_run_id` and the progression boundary intact.
    ///
    /// Crash-recovery-only (non-incremental) keyset needs this at the start of a
    /// FRESH run: `last_cursor_value` is a run-independent persistent field, so a
    /// prior COMPLETED run leaves its final high-water mark behind. If this fresh
    /// run then crashes BEFORE its first page commits, the recovery run would load
    /// that stale mark as this run's "resume point" and skip the whole table
    /// (`WHERE key > <prior-max>` → 0 rows → a successful empty manifest). Clearing
    /// it here ties crash-recovery to THIS run's committed progress only.
    /// Incremental keyset deliberately does NOT clear it — continuing from the
    /// prior high-water mark is the whole point of `keyset_incremental`.
    pub fn clear_cursor_value(&self, export_name: &str, scope: &str) -> Result<()> {
        self.claim_legacy_row(export_name, scope)?;
        self.execute(
            "UPDATE export_state SET last_cursor_value = NULL WHERE export_name = ?1 AND prefix = ?2",
            &[export_name.into(), scope.into()],
        )?;
        Ok(())
    }

    /// Return an export to a "never ran" state.
    ///
    /// Clears the incremental cursor (`export_state`) **and** the committed /
    /// verified boundary (`export_progression`). Both must go: a surviving
    /// progression row would make `rivet state progression` report a stale
    /// committed boundary after `state show` is already empty.
    ///
    /// Only the row of `source` (and the row written before sources were recorded) goes: an export of the same name reading another source keeps its cursor.
    pub fn reset(&self, export_name: &str, source: &str) -> Result<()> {
        self.adopt_respelled_source(export_name, source)?;
        self.execute(
            "DELETE FROM export_state WHERE export_name = ?1 AND prefix IN (?2, '')",
            &[export_name.into(), source.into()],
        )?;
        self.delete_progression(export_name)?;
        Ok(())
    }

    /// Re-bind the stored progress of `key` to the stream, schema and rows it reads now, on the operator's word: the cursor value, its column and any anchor stay. Refused for a cursor written for another column; changes a row only where a run of `key` is refused for its stream.
    pub fn accept(&self, key: &ProgressKey) -> Result<Accepted> {
        let (export_name, scope) = (key.export_name.as_str(), key.source.as_str());
        self.claim_legacy_row(export_name, scope)?;
        let Some((held, stored)) = self.stored_progress(key)?.held else {
            return Ok(Accepted::NoProgress);
        };
        if let Some(column) = &key.column {
            self.get_owned(export_name, scope, column)?;
        }
        let Some((was, now)) = stored.another_than(key) else {
            return Ok(Accepted::Unchanged);
        };
        self.execute(
            "UPDATE export_state SET stream = ?3, source_schema = ?4, population = ?5 \
             WHERE export_name = ?1 AND prefix = ?2",
            &[
                export_name.into(),
                scope.into(),
                key.stream.as_str().into(),
                key.schema.as_str().into(),
                key.population.as_str().into(),
            ],
        )?;
        Ok(Accepted::Rebound { held, was, now })
    }

    pub fn list_all(&self) -> Result<Vec<CursorState>> {
        self.query(
            "SELECT export_name, last_cursor_value, last_run_at, cursor_column FROM export_state \
             WHERE last_cursor_value IS NOT NULL OR resume_run_id IS NOT NULL \
             OR destination IS NULL ORDER BY export_name",
            &[],
            |r| CursorState {
                export_name: r.text(0),
                last_cursor_value: r.opt_text(1),
                last_run_at: r.opt_text(2),
                cursor_column: r.opt_text(3),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    fn key(export: &str, scope: &str, column: &str, stream: &str) -> ProgressKey {
        ProgressKey {
            export_name: export.into(),
            source: scope.into(),
            stream: stream.into(),
            schema: String::new(),
            population: String::new(),
            column: Some(column.into()),
            mode: "keyset",
            continues_high_water: true,
            resumable: true,
        }
    }

    fn put(
        s: &StateStore,
        export: &str,
        scope: &str,
        value: &str,
        column: &str,
        stream: &str,
    ) -> Result<()> {
        s.update_with_column(&key(export, scope, column, stream), value)
    }

    fn anchor(s: &StateStore, export: &str, scope: &str, run_id: &str, stream: &str) -> Result<()> {
        s.set_resume_run_id(&key(export, scope, "id", stream), run_id)
    }

    fn stream_refusal(s: &StateStore, stream: &str, continues: bool) -> Option<String> {
        s.claim(ProgressKey {
            mode: "keyset",
            continues_high_water: continues,
            ..key("orders", "pg/db", "id", stream)
        })
        .err()
        .map(|e| {
            assert_eq!(
                crate::error::error_code(&e),
                Some("RIVET_STATE_CURSOR_STREAM_MISMATCH")
            );
            assert_eq!(crate::error::classify_exit(&e), 5);
            e.to_string()
        })
    }

    fn as_mode(mode: &'static str, scope: &str) -> ProgressKey {
        ProgressKey {
            mode,
            continues_high_water: mode == "incremental",
            ..key("orders", scope, "id", "orders_a")
        }
    }

    /// The refusal `claim` or the claimed lookup gives, checked for `code` and exit 5.
    fn refusal<T>(r: Result<T>, code: &str) -> String {
        let e = r.err().expect("refused");
        assert_eq!(crate::error::error_code(&e), Some(code));
        assert_eq!(crate::error::classify_exit(&e), 5);
        e.to_string()
    }

    const OWNER: &str = "RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH";
    const UNKNOWN: &str = "RIVET_STATE_CHUNK_RUN_OWNER_UNKNOWN";

    #[test]
    fn an_interrupted_keyset_run_is_refused_for_another_mode_every_time_until_it_is_abandoned() {
        let s = store();
        s.set_resume_run_id(&as_mode("keyset", "pg/db"), "run_7")
            .unwrap();
        put(&s, "orders", "pg/db", "100", "id", "orders_a").unwrap();
        for mode in ["incremental", "full", "chunked", "timewindow"] {
            for cycle in 1..=2 {
                let said = refusal(s.claim(as_mode(mode, "pg/db")), OWNER);
                for want in [
                    "run run_7 of mode `keyset`",
                    &format!("runs as `{mode}`"),
                    "restore the `keyset` settings",
                    "`rivet state reset -c <config> --export orders` abandons it",
                ] {
                    assert!(
                        said.contains(want),
                        "{mode} cycle {cycle}: {want} in {said}"
                    );
                }
            }
        }
        let own = s.claim(as_mode("keyset", "pg/db")).expect("its own mode");
        assert_eq!(own.resume_run_id().unwrap().as_deref(), Some("run_7"));
        assert!(
            s.claim(as_mode("incremental", "my/db")).is_ok(),
            "another source holds no interrupted run"
        );
        s.reset("orders", "pg/db").unwrap();
        assert!(s.claim(as_mode("incremental", "pg/db")).is_ok());
    }

    #[test]
    fn a_finished_keyset_run_leaves_a_high_water_any_mode_may_meet() {
        let s = store();
        s.set_resume_run_id(&as_mode("keyset", "pg/db"), "run_7")
            .unwrap();
        put(&s, "orders", "pg/db", "100", "id", "orders_a").unwrap();
        s.clear_resume_run_id("orders", "pg/db").unwrap();
        let claim = s.claim(as_mode("incremental", "pg/db")).expect("MT2");
        assert_eq!(
            claim.cursor().unwrap().last_cursor_value.as_deref(),
            Some("100")
        );
        s.set_resume_run_id(&as_mode("chunked", "pg/db"), "run_8")
            .unwrap();
        s.clear_resume_run_id_every_scope("orders").unwrap();
        assert!(
            s.claim(as_mode("incremental", "pg/db")).is_ok(),
            "a cleared anchor leaves no owner behind"
        );
    }

    #[test]
    fn an_anchor_written_before_owners_were_recorded_is_a_keyset_run() {
        let s = store();
        s.set_resume_run_id(&as_mode("keyset", "pg/db"), "run_7")
            .unwrap();
        s.exec_for_test("UPDATE export_state SET resume_owner = NULL");
        s.exec_for_test(
            "INSERT INTO keyset_range (export_name, run_id, range_index, done, updated_at) \
             VALUES ('orders', 'run_7', 0, 0, 'then'), ('orders', 'run_dead', 1, 0, 'then')",
        );
        let said = refusal(s.claim(as_mode("incremental", "pg/db")), OWNER);
        assert!(said.contains("of mode `keyset`"), "{said}");
        s.claim(as_mode("keyset", "pg/db")).expect("its own mode");
        let owned = |sql: &str| s.query(sql, &[], |r| r.opt_text(0)).unwrap();
        assert_eq!(
            owned("SELECT resume_owner FROM export_state"),
            vec![Some("keyset".to_string())]
        );
        assert_eq!(
            owned("SELECT source FROM keyset_range ORDER BY range_index"),
            vec![Some("pg/db".to_string()), None],
            "only the anchored run's ranges take the source"
        );
    }

    #[test]
    fn an_interrupted_chunk_run_is_refused_for_another_mode_until_finished_or_abandoned() {
        let s = store();
        let chunked = ProgressKey::chunked("orders", "pg/db");
        s.open_chunk_run(&chunked, "run_c", "h", 3, &[(1, 10)])
            .unwrap();
        for cycle in 1..=2 {
            let said = refusal(s.claim(as_mode("incremental", "pg/db")), OWNER);
            for want in [
                "run run_c of mode `chunked`",
                "`rivet state reset-chunks -c <config> --export orders` abandons it",
            ] {
                assert!(said.contains(want), "cycle {cycle}: {want} in {said}");
            }
        }
        let repointed = ProgressKey {
            stream: "orders_b".into(),
            ..chunked.clone()
        };
        assert!(
            s.claim(repointed).is_ok(),
            "a chunk run is compared by its plan hash, not by the stream"
        );
        s.reset_chunk_checkpoint("orders").unwrap();
        assert!(s.claim(as_mode("incremental", "pg/db")).is_ok());

        s.open_chunk_run(&chunked, "run_d", "h", 3, &[(1, 10)])
            .unwrap();
        assert!(s.claim(as_mode("incremental", "pg/db")).is_err());
        s.finalize_chunk_run_completed("run_d").unwrap();
        assert!(s.claim(as_mode("incremental", "pg/db")).is_ok());
    }

    /// `mode` with its checkpoint off: the plan that continues no interrupted run.
    fn uncheckpointed(mode: &'static str, scope: &str) -> ProgressKey {
        ProgressKey {
            resumable: false,
            ..as_mode(mode, scope)
        }
    }

    #[test]
    fn an_interrupted_run_is_refused_for_its_own_mode_without_the_checkpoint_until_finished_or_abandoned()
     {
        type Abandon = fn(&StateStore);
        let cases: [(&'static str, &str, &str, Abandon); 2] = [
            (
                "chunked",
                "(`chunk_checkpoint: true`)",
                "`rivet state reset-chunks -c <config> --export orders` abandons it",
                |s| {
                    s.reset_chunk_checkpoint("orders").unwrap();
                },
            ),
            (
                "keyset",
                "`keyset_incremental: true` or MongoDB's `source.mongo.resume: true`, whichever it had)",
                "`rivet state reset -c <config> --export orders` abandons it",
                |s| s.reset("orders", "pg/db").unwrap(),
            ),
        ];
        for (mode, setting, command, abandon) in cases {
            let s = store();
            let open = |run: &str| match mode {
                "chunked" => s
                    .open_chunk_run(&as_mode(mode, "pg/db"), run, "h", 3, &[(1, 10)])
                    .unwrap(),
                _ => s.set_resume_run_id(&as_mode(mode, "pg/db"), run).unwrap(),
            };
            open("run_1");
            for cycle in 1..=2 {
                let said = refusal(s.claim(uncheckpointed(mode, "pg/db")), OWNER);
                for want in [
                    format!("run run_1 of mode `{mode}` is unfinished"),
                    "runs without the checkpoint that run was opened with".to_string(),
                    "nothing was read or written".to_string(),
                    "restore the checkpoint setting (`chunk_checkpoint: true`".to_string(),
                    setting.to_string(),
                    "run once to finish run run_1, then remove it".to_string(),
                    command.to_string(),
                ] {
                    assert!(
                        said.contains(&want),
                        "{mode} cycle {cycle}: {want} in {said}"
                    );
                }
            }
            let own = s.claim(as_mode(mode, "pg/db")).expect("the checkpoint on");
            assert_eq!(own.resume_run_id().unwrap().as_deref(), Some("run_1"));
            assert!(
                s.claim(uncheckpointed(mode, "my/db")).is_ok(),
                "{mode}: another source holds no interrupted run"
            );
            abandon(&s);
            assert!(
                s.claim(uncheckpointed(mode, "pg/db")).is_ok(),
                "{mode}: abandoned"
            );

            open("run_2");
            assert!(s.claim(uncheckpointed(mode, "pg/db")).is_err());
            match mode {
                "chunked" => s.finalize_chunk_run_completed("run_2").unwrap(),
                _ => s.clear_resume_run_id("orders", "pg/db").unwrap(),
            }
            assert!(
                s.claim(uncheckpointed(mode, "pg/db")).is_ok(),
                "{mode}: finished"
            );
        }
    }

    #[test]
    fn a_chunk_run_with_no_anchor_is_still_an_unfinished_run() {
        let s = store();
        let chunked = ProgressKey::chunked("orders", "pg/db");
        s.open_chunk_run(&chunked, "run_c", "h", 3, &[(1, 10)])
            .unwrap();
        s.reset("orders", "pg/db").unwrap();
        assert_eq!(s.get_resume_run_id("orders", "pg/db").unwrap(), None);
        for key in [uncheckpointed("chunked", "pg/db"), as_mode("full", "pg/db")] {
            let said = refusal(s.claim(key), OWNER);
            assert!(said.contains("run run_c of mode `chunked`"), "{said}");
        }
        assert!(s.claim(uncheckpointed("chunked", "pg/other")).is_ok());
        let own = s.claim(chunked).unwrap().chunk_run().unwrap();
        assert_eq!(own, Some(("run_c".into(), "h".into())));
    }

    #[test]
    fn a_chunk_run_that_recorded_no_source_is_unfinished_for_the_only_source() {
        let s = store();
        s.create_chunk_run("run_l", "orders", "h", 3).unwrap();
        for cycle in 1..=2 {
            for key in [
                uncheckpointed("chunked", "pg/a"),
                as_mode("incremental", "pg/a"),
            ] {
                let said = refusal(s.claim(key), OWNER);
                assert!(
                    said.contains("run run_l of mode `chunked`")
                        && said.contains("`rivet state reset-chunks -c <config> --export orders`"),
                    "cycle {cycle}: {said}"
                );
            }
            assert!(
                s.in_progress_chunk_run("orders", None).unwrap().is_some(),
                "cycle {cycle}: a refusal adopts nothing"
            );
        }
        put(&s, "orders", "pg/b", "5", "id", "orders_b").unwrap();
        for key in [
            uncheckpointed("chunked", "pg/a"),
            as_mode("incremental", "pg/a"),
        ] {
            assert!(
                s.claim(key).is_ok(),
                "with two sources nothing says whose run it is: only resuming it is refused"
            );
        }
        refusal(
            s.claim(ProgressKey::chunked("orders", "pg/a"))
                .unwrap()
                .chunk_run(),
            UNKNOWN,
        );
    }

    #[test]
    fn a_chunk_run_that_recorded_no_source_is_not_resumed_past_a_run_that_finished_after_it() {
        let s = store();
        s.create_chunk_run("run_l", "orders", "h", 3).unwrap();
        let (later, earlier) = ("9999-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00");
        let metric = |export: &str, run: &str, status: &str, at: &str| {
            s.exec_for_test(&format!(
                "INSERT INTO export_metrics (export_name, run_at, duration_ms, total_rows, status, run_id) \
                 VALUES ('{export}', '{at}', 1, 1, '{status}', '{run}')"
            ))
        };
        metric("orders", "run_l", "success", later);
        metric("orders", "run_failed", "failed", later);
        metric("orders", "run_before", "success", earlier);
        metric("users", "run_other", "success", later);
        let chunked = ProgressKey::chunked("orders", "pg/a");
        assert!(
            s.claim(chunked.clone()).is_ok(),
            "its own row, a failed run, an earlier run and another export finish nothing after it"
        );

        metric("orders", "run_n", "success", later);
        for cycle in 1..=2 {
            for key in [
                chunked.clone(),
                uncheckpointed("chunked", "pg/a"),
                as_mode("incremental", "pg/a"),
            ] {
                let said = refusal(s.claim(key), "RIVET_STATE_CHUNK_RUN_SUPERSEDED");
                for want in [
                    "chunk checkpoint run 'run_l' was left in progress by rivet 0.31 or earlier",
                    "run 'run_n' of this export finished after it was opened",
                    "nothing was read or written",
                    "`rivet state reset-chunks -c <config> --export orders` abandons run 'run_l'",
                ] {
                    assert!(said.contains(want), "cycle {cycle}: {want} in {said}");
                }
            }
            assert!(
                s.in_progress_chunk_run("orders", None).unwrap().is_some(),
                "cycle {cycle}: a refusal changes nothing"
            );
        }
        s.reset_chunk_checkpoint("orders").unwrap();
        assert!(s.claim(chunked.clone()).is_ok(), "the remedy lifts it");

        s.open_chunk_run(&chunked, "run_s", "h", 3, &[(1, 10)])
            .unwrap();
        metric("orders", "run_b", "success", later);
        assert!(
            s.claim(chunked).is_ok(),
            "a run that recorded its source is not judged by a same-named export's runs"
        );
    }

    #[test]
    fn a_chunk_run_is_handed_to_its_own_source_only() {
        let s = store();
        let (a, b) = (
            ProgressKey::chunked("orders", "pg/a"),
            ProgressKey::chunked("orders", "pg/b"),
        );
        s.open_chunk_run(&a, "run_a", "h", 3, &[(1, 10)]).unwrap();
        let other = s.claim(b.clone()).unwrap();
        assert_eq!(other.chunk_run().unwrap(), None, "P-02");
        assert_eq!(other.latest_chunk_run().unwrap(), None);
        s.open_chunk_run(&b, "run_b", "h", 3, &[(1, 10)])
            .expect("one in-progress run per source");
        s.open_chunk_run(&b, "run_b2", "h", 3, &[(1, 10)])
            .expect_err("and no second one");
        let own = |k: &ProgressKey| s.claim(k.clone()).unwrap().chunk_run().unwrap();
        assert_eq!(own(&a), Some(("run_a".into(), "h".into())));
        assert_eq!(own(&b), Some(("run_b".into(), "h".into())));
        s.finalize_chunk_run_completed("run_a").unwrap();
        assert_eq!(own(&a), None);
        assert_eq!(own(&b), Some(("run_b".into(), "h".into())));
        let latest = |k: &ProgressKey| s.claim(k.clone()).unwrap().latest_chunk_run().unwrap();
        assert_eq!(latest(&a).as_deref(), Some("run_a"));
        assert_eq!(latest(&b).as_deref(), Some("run_b"));
    }

    #[test]
    fn a_chunk_run_that_recorded_no_source_is_adopted_once_by_the_only_source() {
        let s = store();
        s.create_chunk_run("run_l", "orders", "h", 3).unwrap();
        put(&s, "orders", "pg/a", "5", "id", "orders_a").unwrap();
        put(&s, "other", "pg/b", "5", "id", "other").unwrap();
        let a = ProgressKey::chunked("orders", "pg/a");
        let claim = s.claim(a.clone()).unwrap();
        assert_eq!(claim.latest_chunk_run().unwrap().as_deref(), Some("run_l"));
        assert_eq!(
            claim.chunk_run().unwrap(),
            Some(("run_l".into(), "h".into()))
        );
        assert_eq!(s.in_progress_chunk_run("orders", None).unwrap(), None);
        assert_eq!(claim.resume_run_id().unwrap().as_deref(), Some("run_l"));
        assert_eq!(
            s.claim(ProgressKey::chunked("orders", "pg/b"))
                .unwrap()
                .chunk_run()
                .unwrap(),
            None,
            "once adopted it is its owner's"
        );
        refusal(s.claim(as_mode("incremental", "pg/a")), OWNER);
    }

    #[test]
    fn a_chunk_run_that_recorded_no_source_is_refused_while_the_name_has_two_sources() {
        for other in ["cursor", "chunk_run", "keyset_range"] {
            let s = store();
            s.create_chunk_run("run_l", "orders", "h", 3).unwrap();
            match other {
                "cursor" => put(&s, "orders", "pg/b", "5", "id", "orders_b").unwrap(),
                "chunk_run" => s
                    .open_chunk_run(
                        &ProgressKey::chunked("orders", "pg/b"),
                        "run_b",
                        "h",
                        3,
                        &[],
                    )
                    .unwrap(),
                _ => s
                    .persist_keyset_ranges("orders", "pg/b", "run_k", "id", &[(None, None)])
                    .unwrap(),
            }
            let a = s.claim(ProgressKey::chunked("orders", "pg/a")).unwrap();
            for cycle in 1..=2 {
                let said = refusal(a.chunk_run(), UNKNOWN);
                for want in [
                    "'run_l'",
                    "(`pg/b` and `pg/a`)",
                    "`rivet state reset-chunks -c <config> --export orders` abandons run 'run_l'",
                ] {
                    assert!(
                        said.contains(want),
                        "{other} cycle {cycle}: {want} in {said}"
                    );
                }
                assert!(
                    s.in_progress_chunk_run("orders", None).unwrap().is_some(),
                    "{other} cycle {cycle}: a refusal changes nothing"
                );
            }
            s.reset_chunk_checkpoint("orders").unwrap();
            assert_eq!(a.chunk_run().unwrap(), None, "{other}: the remedy lifts it");
        }
    }

    #[test]
    fn a_stored_cursor_is_refused_for_another_table_every_time_until_a_reset() {
        let s = store();
        put(&s, "orders", "pg/db", "110", "id", "orders_a").unwrap();
        assert_eq!(stream_refusal(&s, "orders_a", true), None, "its own stream");
        for cycle in 1..=2 {
            let said = stream_refusal(&s, "orders_b", true).expect("refused");
            for want in ["`orders_a`", "`orders_b`", "cursor `110`", "own names"] {
                assert!(said.contains(want), "cycle {cycle}: {want} in {said}");
            }
            assert!(
                said.contains("`rivet state reset -c <config> --export orders`"),
                "{said}"
            );
        }
        assert_eq!(
            s.get("orders", "pg/db")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("110"),
            "a refusal changes nothing"
        );
        s.reset("orders", "pg/db").unwrap();
        assert_eq!(stream_refusal(&s, "orders_b", true), None);
    }

    #[test]
    fn a_claim_hands_out_the_progress_of_its_own_key_only() {
        let s = store();
        put(&s, "orders", "pg/db", "110", "id", "orders_a").unwrap();
        anchor(&s, "orders", "pg/db", "run_7", "orders_a").unwrap();
        put(&s, "orders", "my/db", "5", "id", "orders_a").unwrap();
        put(&s, "users", "pg/db", "9", "id", "users").unwrap();

        let own = s.claim(key("orders", "pg/db", "id", "orders_a")).unwrap();
        assert_eq!(own.key(), &key("orders", "pg/db", "id", "orders_a"));
        assert_eq!(
            own.cursor().unwrap().last_cursor_value.as_deref(),
            Some("110")
        );
        assert_eq!(own.resume_run_id().unwrap().as_deref(), Some("run_7"));

        let other_source = s.claim(key("orders", "my/db", "id", "orders_a")).unwrap();
        assert_eq!(
            other_source.cursor().unwrap().last_cursor_value.as_deref(),
            Some("5")
        );
        assert_eq!(other_source.resume_run_id().unwrap(), None);

        let other_column = s.claim(key("orders", "pg/db", "ts", "orders_a")).unwrap();
        let e = other_column.cursor().unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_STATE_CURSOR_OWNER_MISMATCH")
        );
        assert!(e.to_string().contains("written for `id`"), "{e}");
    }

    #[test]
    fn a_cursor_write_for_a_key_with_no_progress_column_is_refused_and_stores_nothing() {
        let s = store();
        let keyless = ProgressKey {
            column: None,
            ..key("orders", "pg/db", "id", "orders_a")
        };
        let e = s.update_with_column(&keyless, "110").unwrap_err();
        assert!(
            e.to_string()
                .contains("committed cursor `110` but its strategy has no cursor identity"),
            "{e}"
        );
        assert!(s.list_all().unwrap().is_empty());
    }

    #[test]
    fn a_stale_cursor_no_clean_run_seeks_from_is_not_progress_but_an_anchor_is() {
        let s = store();
        put(&s, "orders", "pg/db", "110", "id", "orders_a").unwrap();
        assert_eq!(stream_refusal(&s, "orders_b", false), None);
        anchor(&s, "orders", "pg/db", "run_7", "orders_a").unwrap();
        let said = stream_refusal(&s, "orders_b", false).expect("an interrupted run is progress");
        assert!(said.contains("interrupted run run_7"), "{said}");
        assert_eq!(stream_refusal(&s, "orders_a", false), None);
    }

    #[test]
    fn an_anchor_records_its_stream_before_any_cursor_exists() {
        let s = store();
        anchor(&s, "orders", "pg/db", "run_1", "orders_a").unwrap();
        assert!(stream_refusal(&s, "orders_b", true).is_some());
        assert_eq!(
            s.claim(key("orders", "my/db", "id", "orders_b"))
                .ok()
                .map(|_| ()),
            Some(()),
            "another source scope holds no progress"
        );
    }

    #[test]
    fn progress_written_before_streams_were_recorded_is_adopted_once_then_guarded() {
        let s = store();
        put(&s, "orders", "pg/db", "110", "id", "orders_a").unwrap();
        s.execute("UPDATE export_state SET stream = NULL", &[])
            .unwrap();
        assert_eq!(
            stream_refusal(&s, "orders_b", true),
            None,
            "adopted, not refused"
        );
        assert_eq!(
            s.get("orders", "pg/db")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("110"),
            "adopted, not reset"
        );
        assert!(
            stream_refusal(&s, "orders_a", true).is_some(),
            "now `orders_b`'s"
        );
        assert_eq!(stream_refusal(&s, "orders_b", true), None);
    }

    #[test]
    fn a_qualifier_added_to_the_same_name_is_not_another_stream_but_another_qualifier_is() {
        for (stored, now, differ) in [
            ("orders", "orders", false),
            ("orders", "public.orders", false),
            ("shop.orders", "orders", false),
            ("a.orders", "b.orders", true),
            ("orders", "reorders", true),
            ("reorders", "orders", true),
            ("orders", "public.orders_b", true),
            ("", "orders", false),
            ("orders", "", false),
        ] {
            assert_eq!(streams_differ(stored, now), differ, "{stored} vs {now}");
        }
    }

    #[test]
    fn a_query_naming_no_relation_is_never_refused_or_adopted() {
        let s = store();
        put(&s, "orders", "pg/db", "110", "id", "orders_a").unwrap();
        assert_eq!(stream_refusal(&s, "", true), None, "table then query");
        put(&s, "orders", "pg/db", "120", "id", "").unwrap();
        assert_eq!(
            stream_refusal(&s, "orders_b", true),
            None,
            "query then table"
        );
        s.execute("UPDATE export_state SET stream = NULL", &[])
            .unwrap();
        assert_eq!(stream_refusal(&s, "", true), None);
        assert!(
            stream_refusal(&s, "orders_b", true).is_none()
                && stream_refusal(&s, "orders_a", true).is_some(),
            "a query run adopted nothing; the first table did"
        );
    }

    #[test]
    fn two_configs_with_one_export_name_keep_separate_cursors() {
        let s = store();
        put(&s, "orders", "pg/out", "2026-09-01", "updated_at", "").unwrap();
        assert_eq!(
            s.get("orders", "my/out").unwrap().last_cursor_value,
            None,
            "another destination starts from nothing"
        );
        put(&s, "orders", "my/out", "2026-01-01", "updated_at", "").unwrap();
        assert_eq!(
            s.get("orders", "pg/out")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("2026-09-01")
        );
        assert_eq!(
            s.get("orders", "my/out")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("2026-01-01")
        );
        anchor(&s, "orders", "pg/out", "r1", "").unwrap();
        assert_eq!(s.get_resume_run_id("orders", "my/out").unwrap(), None);
    }

    #[test]
    fn a_legacy_cursor_is_continued_by_the_first_scope_that_writes_and_by_no_other() {
        let s = store();
        s.update_legacy("orders", "100").unwrap();
        assert_eq!(
            s.get("orders", "a/out")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("100"),
            "read before any claim"
        );
        put(&s, "orders", "a/out", "200", "id", "").unwrap();
        assert_eq!(
            s.get("orders", "a/out")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("200")
        );
        assert_eq!(
            s.get("orders", "b/out").unwrap().last_cursor_value,
            None,
            "claimed: no other scope inherits it"
        );
        assert_eq!(s.list_all().unwrap().len(), 1, "moved, not copied");
    }

    const MASKED: &str = "postgres://host=h user=u password=*** dbname=d";
    const UNMASKED: &str = "postgres://host=h user=u password=s3cr3tpw dbname=d";

    fn scopes(s: &StateStore) -> Vec<String> {
        s.query(
            "SELECT prefix FROM export_state ORDER BY prefix",
            &[],
            |r| r.text(0),
        )
        .unwrap()
    }

    const SPELLED: &str = "postgres://db.host:5432/app";
    const PORTLESS: &str = "postgres://db.host/app";
    const OTHER_PORT: &str = "postgres://db.host:6000/app";

    fn column(s: &StateStore, sql: &str) -> Vec<String> {
        s.query(sql, &[], |r| r.opt_text(0).unwrap_or_else(|| "NULL".into()))
            .unwrap()
    }

    /// P-18: a cursor stored under a URL that left the default port out is the same source's and continues.
    #[test]
    fn a_cursor_stored_without_the_default_port_continues_under_the_spelled_one() {
        let s = store();
        put(&s, "orders", PORTLESS, "100", "id", "orders_a").unwrap();
        put(&s, "orders", OTHER_PORT, "7", "id", "orders_a").unwrap();
        put(&s, "users", PORTLESS, "55", "id", "users").unwrap();
        let claim = s.claim(as_mode("incremental", SPELLED)).unwrap();
        assert_eq!(
            claim.cursor().unwrap().last_cursor_value.as_deref(),
            Some("100")
        );
        assert_eq!(
            scopes(&s),
            [PORTLESS, SPELLED, OTHER_PORT],
            "this export's row moved; another port and another export are untouched"
        );
        let other = s.claim(as_mode("incremental", OTHER_PORT)).unwrap();
        assert_eq!(
            other.cursor().unwrap().last_cursor_value.as_deref(),
            Some("7")
        );

        let s = store();
        put(&s, "orders", PORTLESS, "100", "id", "orders_a").unwrap();
        put(&s, "orders", SPELLED, "300", "id", "orders_a").unwrap();
        let claim = s.claim(as_mode("incremental", SPELLED)).unwrap();
        assert_eq!(
            claim.cursor().unwrap().last_cursor_value.as_deref(),
            Some("300"),
            "a row already under the spelled key outranks the other spelling"
        );
        assert_eq!(scopes(&s), [SPELLED]);
    }

    /// P-18 for the checkpoints: an interrupted keyset or range-chunk run opened without the port is resumed, not left beside a fresh pass.
    #[test]
    fn an_interrupted_run_stored_without_the_default_port_is_resumed_under_the_spelled_one() {
        let s = store();
        anchor(&s, "orders", PORTLESS, "k1", "orders_a").unwrap();
        s.persist_keyset_ranges("orders", PORTLESS, "k1", "id", &[(None, None)])
            .unwrap();
        let claim = s.claim(as_mode("keyset", SPELLED)).unwrap();
        assert_eq!(claim.resume_run_id().unwrap().as_deref(), Some("k1"));
        assert_eq!(column(&s, "SELECT source FROM keyset_range"), [SPELLED]);

        let s = store();
        s.create_chunk_run("c1", "orders", "plan", 3).unwrap();
        s.create_chunk_run("c0", "users", "plan", 3).unwrap();
        s.execute("UPDATE chunk_run SET source = ?1", &[PORTLESS.into()])
            .unwrap();
        let claim = s.claim(ProgressKey::chunked("orders", SPELLED)).unwrap();
        assert_eq!(
            claim.chunk_run().unwrap(),
            Some(("c1".to_string(), "plan".to_string()))
        );
        assert_eq!(
            column(&s, "SELECT source FROM chunk_run ORDER BY run_id"),
            [PORTLESS, SPELLED],
            "another export's run is not this claim's to move"
        );

        let s = store();
        s.create_chunk_run("old", "orders", "plan", 3).unwrap();
        s.execute("UPDATE chunk_run SET source = ?1", &[PORTLESS.into()])
            .unwrap();
        s.create_chunk_run("new", "orders", "plan", 3).unwrap();
        s.execute(
            "UPDATE chunk_run SET source = ?1 WHERE run_id = 'new'",
            &[SPELLED.into()],
        )
        .unwrap();
        s.persist_keyset_ranges("orders", PORTLESS, "k1", "id", &[(None, None)])
            .unwrap();
        s.persist_keyset_ranges("orders", SPELLED, "k2", "id", &[(None, None)])
            .unwrap();
        s.adopt_respelled_source("orders", SPELLED).unwrap();
        assert_eq!(
            column(&s, "SELECT source FROM chunk_run ORDER BY run_id"),
            [SPELLED, PORTLESS],
            "one unfinished run per source: the spelled one is kept"
        );
        assert_eq!(
            column(&s, "SELECT source FROM keyset_range ORDER BY run_id"),
            [PORTLESS, SPELLED]
        );
    }

    /// P-19: a reset clears the row of its own source, under either spelling, and no other source's.
    #[test]
    fn a_reset_clears_its_own_source_and_keeps_another_sources_cursor() {
        let s = store();
        put(&s, "orders", PORTLESS, "100", "id", "orders_a").unwrap();
        put(&s, "orders", OTHER_PORT, "7", "id", "orders_a").unwrap();
        s.update_legacy("orders", "1").unwrap();
        for cycle in 1..=2 {
            s.reset("orders", SPELLED).unwrap();
            assert_eq!(scopes(&s), [OTHER_PORT], "cycle {cycle}");
        }
        let kept = s.get("orders", OTHER_PORT).unwrap();
        assert_eq!(kept.last_cursor_value.as_deref(), Some("7"));
    }

    const SPENT_1: &str = "from ? where spent = 1";
    const SPENT_0: &str = "from ? where spent = 0";

    fn reading(stream: &str, schema: &str, population: &str) -> ProgressKey {
        ProgressKey {
            stream: stream.into(),
            schema: schema.into(),
            population: population.into(),
            ..as_mode("incremental", "pg/db")
        }
    }

    /// An edited filter or another schema under the same name is another stream: refused twice by code, continued once restored, started over after a reset.
    #[test]
    fn an_edited_filter_or_another_schema_is_refused_until_it_is_restored_or_reset() {
        let s = store();
        let wrote = reading("orders", "", SPENT_1);
        s.update_with_column(&wrote, "10").unwrap();
        assert_eq!(
            column(
                &s,
                "SELECT stream || '|' || source_schema || '|' || population FROM export_state"
            ),
            [format!("orders||{SPENT_1}")],
            "every cursor write records what it read"
        );
        let own_search_path = "orders under the server's own search_path";
        let edits = [
            (
                reading("orders", "", SPENT_0),
                format!("orders ({SPENT_1})"),
                format!("orders ({SPENT_0})"),
            ),
            (
                reading("orders", "sales", SPENT_1),
                own_search_path.to_string(),
                "orders under search_path sales".to_string(),
            ),
            (
                reading("hr.orders", "", SPENT_1),
                "orders".to_string(),
                "hr.orders".to_string(),
            ),
        ];
        for (edited, was, now) in &edits[..2] {
            for cycle in 1..=2 {
                let said = refusal(
                    s.claim(edited.clone()),
                    "RIVET_STATE_CURSOR_STREAM_MISMATCH",
                );
                for want in [
                    "its stored progress (cursor `10`)",
                    &format!("was written reading `{was}`"),
                    &format!("this export reads `{now}`"),
                    "two exports sharing a name in one state database need their own names",
                    "restore what it read to continue from the stored progress",
                    &format!("`rivet state reset -c <config> --export orders` starts `{now}` over"),
                    "it discards the progress 'orders' holds on this source",
                ] {
                    assert!(said.contains(want), "cycle {cycle}: {want} in {said}");
                }
            }
        }
        let (qualified, _, _) = &edits[2];
        assert!(
            s.claim(qualified.clone()).is_ok(),
            "a bare stored name may be the qualified one, as before"
        );
        let restored = s.claim(wrote.clone()).expect("what it read, restored");
        assert_eq!(
            restored.cursor().unwrap().last_cursor_value.as_deref(),
            Some("10")
        );
        s.reset("orders", "pg/db").unwrap();
        let fresh = s.claim(edits[0].0.clone()).expect("after the reset");
        assert_eq!(fresh.cursor().unwrap().last_cursor_value, None);
    }

    /// A query as written and a substituted query that fills its placeholders are one row set; two templates, or two substituted queries, are one only when equal.
    #[test]
    fn a_substituted_query_is_the_rows_of_the_template_it_fills() {
        let written = "from ? where region = '${region}' and day >= ${from} order by id";
        let filled = "from ? where region = 'eu' and day >= 20 order by id";
        for (a, b, same) in [
            (written, filled, true),
            (written, written, true),
            (filled, filled, true),
            (written, "from ? where region = 'eu' and day >= 20", false),
            (written, "from ? where region = 'eu' order by id", false),
            (
                written,
                "from ? where day >= 20 and region = 'eu' order by id",
                false,
            ),
            (
                written,
                "x from ? where region = 'eu' and day >= 20 order by id",
                false,
            ),
            (
                filled,
                "from ? where region = 'us' and day >= 20 order by id",
                false,
            ),
            (
                "from ? where a = ${x}",
                "from ? where a = ${x} and id > 0",
                false,
            ),
            ("from ? where a = ${x}", "from ? where a = ${y}", false),
            ("from ? where ${f}", "from ? where anything at all", true),
            ("a${x}b${y}b${z}c", "a1b2b3c", true),
            ("a${x}b${y}b${z}c", "a1b2c", false),
            ("a${x}a", "a", false),
            ("a${x}a", "aa", true),
            ("${x} and ${y}", "1 and 2 and 3", true),
            ("${x} or ${y}", "1 and 2", false),
        ] {
            assert_eq!(same_rows(a, b), same, "{a} / {b}");
            assert_eq!(same_rows(b, a), same, "{b} / {a}");
        }
    }

    /// A plan sealed before templates were recorded carries the substituted query: it continues progress stored under the template, and the template continues progress it stored.
    #[test]
    fn a_plan_sealed_with_its_values_substituted_and_a_template_continue_each_other() {
        let written = "from ? where spent = ${spent}";
        for (stored, now) in [(written, SPENT_1), (SPENT_1, written), (written, SPENT_0)] {
            let s = store();
            s.update_with_column(&reading("orders", "", stored), "10")
                .unwrap();
            let claim = s.claim(reading("orders", "", now)).expect("one row set");
            assert_eq!(
                claim.cursor().unwrap().last_cursor_value.as_deref(),
                Some("10")
            );
            assert_eq!(
                s.accept(&reading("orders", "", now)).unwrap(),
                Accepted::Unchanged
            );
        }
        let s = store();
        s.update_with_column(&reading("orders", "", written), "10")
            .unwrap();
        for edited in [
            "from ? where spent = ${spent} and id > 0",
            "from ? where paid = 1",
        ] {
            refusal(
                s.claim(reading("orders", "", edited)),
                "RIVET_STATE_CURSOR_STREAM_MISMATCH",
            );
        }
    }

    /// The stream parts and the cursor the one `export_state` row holds.
    fn bound(s: &StateStore) -> Vec<String> {
        let parts =
            "stream || '|' || source_schema || '|' || population || '|' || last_cursor_value";
        column(s, &format!("SELECT {parts} FROM export_state"))
    }

    /// `state accept` keeps the cursor under the edited stream: refused twice before, continued twice after, and the row it left is refused for the stream it had.
    #[test]
    fn an_accepted_edit_keeps_the_cursor_under_what_the_export_reads_now() {
        for (edited, now) in [
            (
                reading("orders", "", SPENT_0),
                format!("orders ({SPENT_0})"),
            ),
            (
                reading("orders", "sales", SPENT_1),
                "orders under search_path sales".to_string(),
            ),
            (reading("orders_b", "", SPENT_1), "orders_b".to_string()),
        ] {
            let s = store();
            let wrote = reading("orders", "", SPENT_1);
            s.update_with_column(&wrote, "10").unwrap();
            for cycle in 1..=2 {
                let said = refusal(
                    s.claim(edited.clone()),
                    "RIVET_STATE_CURSOR_STREAM_MISMATCH",
                );
                let accept = format!(
                    "If the edit is meant and the export should go on from that progress, \
                     `rivet state accept -c <config> --export orders` keeps it and records it as \
                     belonging to `{now}` — rows of `{now}` below it are not delivered."
                );
                assert!(said.ends_with(&accept), "cycle {cycle}: {said}");
            }
            let other_export = ProgressKey {
                export_name: "customers".into(),
                ..edited.clone()
            };
            assert_eq!(s.accept(&other_export).unwrap(), Accepted::NoProgress);
            refusal(
                s.claim(edited.clone()),
                "RIVET_STATE_CURSOR_STREAM_MISMATCH",
            );

            let Accepted::Rebound { held, was, now: is } = s.accept(&edited).unwrap() else {
                panic!("the edit is accepted");
            };
            assert_eq!(held, "cursor `10`");
            assert!(was.starts_with("orders"), "{was}");
            assert_eq!(is, now);
            assert_eq!(
                bound(&s),
                [format!(
                    "{}|{}|{}|10",
                    edited.stream, edited.schema, edited.population
                )],
                "the row names what the export reads now and keeps its cursor"
            );
            for cycle in 1..=2 {
                let claim = s.claim(edited.clone());
                let cursor = claim.expect("accepted").cursor().unwrap();
                assert_eq!(
                    cursor.last_cursor_value.as_deref(),
                    Some("10"),
                    "cycle {cycle}"
                );
                assert_eq!(cursor.cursor_column.as_deref(), Some("id"));
            }
            assert_eq!(s.accept(&edited).unwrap(), Accepted::Unchanged);
            refusal(s.claim(wrote), "RIVET_STATE_CURSOR_STREAM_MISMATCH");
        }
    }

    /// `state accept` changes nothing where a run is not refused for its stream, and refuses a cursor written for another column.
    #[test]
    fn an_acceptance_rebinds_only_progress_a_run_is_refused_for() {
        let s = store();
        let wrote = reading("orders", "", SPENT_1);
        assert_eq!(s.accept(&wrote).unwrap(), Accepted::NoProgress, "no row");
        s.update_with_column(&wrote, "10").unwrap();
        let before = bound(&s);
        assert_eq!(s.accept(&wrote).unwrap(), Accepted::Unchanged);

        let full = ProgressKey {
            continues_high_water: false,
            ..reading("orders", "", SPENT_0)
        };
        assert!(
            s.claim(full.clone()).is_ok(),
            "a full pass seeks from no cursor"
        );
        assert_eq!(s.accept(&full).unwrap(), Accepted::NoProgress);

        let other_column = ProgressKey {
            column: Some("updated_at".into()),
            ..reading("orders", "", SPENT_0)
        };
        for cycle in 1..=2 {
            let said = refusal(s.accept(&other_column), "RIVET_STATE_CURSOR_OWNER_MISMATCH");
            assert!(
                said.contains("was written for `id`"),
                "cycle {cycle}: {said}"
            );
        }
        assert_eq!(bound(&s), before, "nothing above changed the row");

        s.execute(
            "UPDATE export_state SET prefix = '', stream = NULL, source_schema = NULL, population = NULL",
            &[],
        )
        .unwrap();
        let edited = reading("orders", "", SPENT_0);
        assert_eq!(
            s.accept(&edited).unwrap(),
            Accepted::Unchanged,
            "a row written before streams were recorded is adopted by the next run, not accepted"
        );
        assert_eq!(scopes(&s), ["pg/db"], "the accept claimed the legacy row");
    }

    /// An acceptance keeps an interrupted run's anchor as it keeps a cursor.
    #[test]
    fn an_accepted_edit_keeps_an_interrupted_runs_anchor() {
        let s = store();
        let keyset = |population: &str| ProgressKey {
            population: population.into(),
            continues_high_water: false,
            ..key("orders", "pg/db", "id", "orders")
        };
        s.set_resume_run_id(&keyset(SPENT_1), "run_7").unwrap();
        refusal(
            s.claim(keyset(SPENT_0)),
            "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        );
        let Accepted::Rebound { held, .. } = s.accept(&keyset(SPENT_0)).unwrap() else {
            panic!("the edit is accepted");
        };
        assert_eq!(held, "interrupted run run_7");
        let claim = s.claim(keyset(SPENT_0)).expect("accepted");
        assert_eq!(claim.resume_run_id().unwrap().as_deref(), Some("run_7"));
    }

    /// A name read through a one-schema search_path is that schema's table: qualifying it by hand continues, another schema's is refused.
    #[test]
    fn a_search_path_resolves_the_unqualified_name_it_is_compared_by() {
        let s = store();
        s.update_with_column(&reading("orders", "sales", "from ?"), "10")
            .unwrap();
        assert!(s.claim(reading("sales.orders", "", "from ?")).is_ok());
        assert!(s.claim(reading("sales.orders", "hr", "from ?")).is_ok());
        let said = refusal(
            s.claim(reading("hr.orders", "", "from ?")),
            "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        );
        assert!(said.contains("reading `sales.orders`"), "{said}");
        assert!(said.contains("reads `hr.orders`"), "{said}");
        let said = refusal(
            s.claim(reading("orders", "", "from ?")),
            "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        );
        assert!(
            said.contains("reading `orders under search_path sales`"),
            "{said}"
        );
        let said = refusal(
            s.claim(reading("orders", "sales,hr", "from ?")),
            "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        );
        assert!(
            said.contains("reads `orders under search_path sales,hr`"),
            "{said}"
        );
        assert_eq!(resolved("orders", "a,b"), "orders");
        assert_eq!(resolved("", "sales"), "");

        let s = store();
        s.update_with_column(&reading("", "sales", "select 1"), "10")
            .unwrap();
        let said = refusal(
            s.claim(reading("", "", "select 1")),
            "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        );
        assert!(
            said.contains("reading `its query under search_path sales`"),
            "{said}"
        );
    }

    /// A row written before the schema and the rows were recorded (rivet <= 0.31, or one that recorded the stream only) is adopted once and compared from then on.
    #[test]
    fn a_row_written_before_the_query_was_recorded_is_adopted_then_compared() {
        for stream in [None, Some("orders")] {
            let s = store();
            s.execute(
                "INSERT INTO export_state (export_name, prefix, last_cursor_value, cursor_column, stream) \
                 VALUES ('orders', 'pg/db', '10', 'id', ?1)",
                &[stream.into()],
            )
            .unwrap();
            let claim = s.claim(reading("orders", "sales", SPENT_1)).unwrap();
            assert_eq!(
                claim.cursor().unwrap().last_cursor_value.as_deref(),
                Some("10"),
                "{stream:?}: the legacy row continues"
            );
            assert_eq!(
                column(
                    &s,
                    "SELECT stream || '|' || source_schema || '|' || population FROM export_state"
                ),
                [format!("orders|sales|{SPENT_1}")],
                "{stream:?}"
            );
            assert!(s.claim(reading("orders", "sales", SPENT_1)).is_ok());
            refusal(
                s.claim(reading("orders", "sales", SPENT_0)),
                "RIVET_STATE_CURSOR_STREAM_MISMATCH",
            );
            refusal(
                s.claim(reading("orders", "", SPENT_1)),
                "RIVET_STATE_CURSOR_STREAM_MISMATCH",
            );
        }
        let s = store();
        s.update_legacy("orders", "10").unwrap();
        s.claim(ProgressKey {
            continues_high_water: true,
            ..ProgressKey::chunked("orders", "pg/db")
        })
        .unwrap();
        assert_eq!(
            column(
                &s,
                "SELECT COALESCE(stream, 'NULL') || '|' || source_schema || '|' || \
                 COALESCE(population, 'NULL') FROM export_state"
            ),
            ["NULL||NULL"],
            "a key that names no stream and no rows records neither"
        );
    }

    /// A cursor stored under the unmasked scope continues under the masked one, and the password leaves the table.
    #[test]
    fn a_cursor_stored_with_its_password_unmasked_is_adopted_by_the_masked_scope() {
        let s = store();
        put(&s, "orders", UNMASKED, "100", "id", "").unwrap();
        anchor(&s, "orders", UNMASKED, "r1", "").unwrap();
        put(&s, "orders", "postgres://other:5432/d", "7", "id", "").unwrap();
        put(&s, "users", UNMASKED, "55", "id", "").unwrap();
        assert_eq!(
            s.get("orders", MASKED)
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("100")
        );
        assert_eq!(
            s.get_resume_run_id("orders", MASKED).unwrap().as_deref(),
            Some("r1")
        );
        assert_eq!(
            scopes(&s),
            [MASKED, UNMASKED, "postgres://other:5432/d"],
            "this export's row moved; another source and another export are untouched"
        );
        assert_eq!(
            s.get("orders", "postgres://other:5432/d")
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("7")
        );
    }

    /// Each reader and the writer adopt on their own: none relies on another having run first.
    #[test]
    fn the_resume_run_id_reader_and_the_writer_adopt_an_unmasked_scope_too() {
        let s = store();
        anchor(&s, "orders", UNMASKED, "r1", "").unwrap();
        assert_eq!(
            s.get_resume_run_id("orders", MASKED).unwrap().as_deref(),
            Some("r1")
        );
        assert_eq!(scopes(&s), [MASKED]);

        let s = store();
        put(&s, "orders", UNMASKED, "100", "id", "").unwrap();
        anchor(&s, "orders", MASKED, "r2", "").unwrap();
        assert_eq!(scopes(&s), [MASKED]);
        assert_eq!(
            s.get("orders", MASKED)
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("100"),
            "the write continued the stored cursor instead of starting a second row"
        );
    }

    /// After a password rotation two unmasked rows exist: the newest is adopted, and a row already under the masked scope outranks both.
    #[test]
    fn the_newest_unmasked_row_is_adopted_and_the_masked_row_outranks_it() {
        let rotated = "postgres://host=h user=u password=rotated dbname=d";
        let stage = |s: &StateStore, scope: &str, value: &str, at: &str| {
            s.execute(
                "INSERT INTO export_state (export_name, prefix, last_cursor_value, last_run_at) \
                 VALUES ('orders', ?1, ?2, ?3)",
                &[scope.into(), value.into(), at.into()],
            )
            .unwrap();
        };
        let s = store();
        stage(&s, UNMASKED, "100", "2026-09-01T00:00:00+00:00");
        stage(&s, rotated, "200", "2026-10-01T00:00:00+00:00");
        assert_eq!(
            s.get("orders", MASKED)
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("200")
        );
        assert_eq!(scopes(&s), [MASKED], "the older copy is deleted");

        let s = store();
        stage(&s, MASKED, "300", "2026-08-01T00:00:00+00:00");
        stage(&s, UNMASKED, "100", "2026-09-01T00:00:00+00:00");
        assert_eq!(
            s.get("orders", MASKED)
                .unwrap()
                .last_cursor_value
                .as_deref(),
            Some("300")
        );
        assert_eq!(scopes(&s), [MASKED]);
    }

    #[test]
    fn a_resume_run_id_in_any_scope_is_seen_until_every_scope_is_cleared() {
        let s = store();
        assert!(!s.has_resume_run_id_in_any_scope("orders").unwrap());
        anchor(&s, "orders", "pg/out", "r1", "").unwrap();
        assert!(s.has_resume_run_id_in_any_scope("orders").unwrap());
        assert!(!s.has_resume_run_id_in_any_scope("other").unwrap());
        s.clear_resume_run_id_every_scope("orders").unwrap();
        assert!(!s.has_resume_run_id_in_any_scope("orders").unwrap());
    }

    #[test]
    fn get_unknown_returns_empty_state() {
        let s = store();
        let state = s.get("nonexistent", "").unwrap();
        assert!(state.last_cursor_value.is_none());
    }

    #[test]
    fn update_then_get_returns_stored_cursor() {
        let s = store();
        s.update_legacy("orders", "2024-06-01").unwrap();
        assert_eq!(
            s.get("orders", "").unwrap().last_cursor_value.as_deref(),
            Some("2024-06-01")
        );
    }

    #[test]
    fn update_overwrites_previous_cursor() {
        let s = store();
        s.update_legacy("orders", "100").unwrap();
        s.update_legacy("orders", "200").unwrap();
        assert_eq!(
            s.get("orders", "").unwrap().last_cursor_value.as_deref(),
            Some("200")
        );
    }

    #[test]
    fn reset_clears_cursor_state() {
        let s = store();
        s.update_legacy("orders", "100").unwrap();
        s.reset("orders", "pg/db").unwrap();
        assert!(s.get("orders", "").unwrap().last_cursor_value.is_none());
    }

    #[test]
    fn clear_cursor_value_nulls_the_cursor_but_keeps_the_resume_run_id() {
        // The keyset stale-cursor fix: a fresh non-incremental run must null the
        // prior COMPLETED run's high-water mark so a pre-first-commit crash cannot
        // resume from it (skipping the whole table) — WITHOUT dropping the fresh
        // resume_run_id it is about to set (crash-recovery needs that).
        let s = store();
        s.update_legacy("orders", "9000000").unwrap();
        anchor(&s, "orders", "", "run_2", "").unwrap();
        s.clear_cursor_value("orders", "").unwrap();
        assert!(
            s.get("orders", "").unwrap().last_cursor_value.is_none(),
            "cursor must be nulled"
        );
        assert_eq!(
            s.get_resume_run_id("orders", "").unwrap().as_deref(),
            Some("run_2"),
            "resume_run_id must survive"
        );
    }

    /// A CDC stream's recorded table set is not a cursor: `state show` lists what it listed before.
    #[test]
    fn list_all_leaves_out_a_cdc_capture_record() {
        let s = store();
        s.update_with_column(&key("orders", "pg/a", "id", "orders"), "7")
            .unwrap();
        let cdc = ProgressKey::cdc("stream", "pg/a", &["t".to_string()]);
        s.record_captured_tables(&cdc, "b/out").unwrap();
        let listed: Vec<String> = s
            .list_all()
            .unwrap()
            .into_iter()
            .map(|c| c.export_name)
            .collect();
        assert_eq!(listed, ["orders"]);
    }

    #[test]
    fn list_all_on_empty_store_returns_empty() {
        assert!(store().list_all().unwrap().is_empty());
    }

    #[test]
    fn list_all_returns_entries_sorted_by_name() {
        let s = store();
        s.update_legacy("gamma", "3").unwrap();
        s.update_legacy("alpha", "1").unwrap();
        s.update_legacy("beta", "2").unwrap();
        let all = s.list_all().unwrap();
        assert_eq!(all[0].export_name, "alpha");
        assert_eq!(all[2].export_name, "gamma");
    }

    // ─── Cursor round-trip / monotonicity (QA backlog Task 3.1) ─────────────
    //
    // ADR-0001 I3 makes monotonicity a pipeline responsibility, not a storage
    // one.  These tests pin the *value-preservation* contract on the state
    // side — the subset the pipeline relies on when reading the stored cursor
    // back on resume.

    /// Duplicate cursor values across runs are common when the cursor column
    /// is a low-precision timestamp with ties.  The store must return each
    /// written value verbatim.
    #[test]
    fn duplicate_cursor_values_are_stored_as_written() {
        let s = store();
        s.update_legacy("orders", "2024-06-01T00:00:00Z").unwrap();
        s.update_legacy("orders", "2024-06-01T00:00:00Z").unwrap();
        assert_eq!(
            s.get("orders", "").unwrap().last_cursor_value.as_deref(),
            Some("2024-06-01T00:00:00Z")
        );
    }

    /// Microsecond/nanosecond precision must not be rounded or truncated on
    /// round-trip — otherwise the pipeline's strict-greater-than boundary
    /// check would re-export rows on the microsecond edge.
    #[test]
    fn high_precision_timestamp_is_preserved_byte_for_byte() {
        let s = store();
        let ts = "2024-06-01T12:34:56.123456789+02:00";
        s.update_legacy("events", ts).unwrap();
        assert_eq!(
            s.get("events", "").unwrap().last_cursor_value.as_deref(),
            Some(ts)
        );
    }

    /// Cursor values can be arbitrary UTF-8: UUID v7, version tokens,
    /// Cyrillic names, multiline strings, the empty string.
    #[test]
    fn unicode_and_binary_like_cursor_values_round_trip() {
        let s = store();
        let values = [
            "2024-06-01",
            "018f1c0b-7a34-7b54-8e16-1c5a9b3f1c2d", // UUID v7
            "ελληνικά 🚀 cursor",
            "v\n\t with whitespace",
            "",
        ];
        for v in values {
            s.update_legacy("t", v).unwrap();
            assert_eq!(
                s.get("t", "").unwrap().last_cursor_value.as_deref(),
                Some(v),
                "cursor value {v:?} must round-trip exactly"
            );
        }
    }

    /// Resume-from-zero tooling depends on `reset` producing a state
    /// indistinguishable from "never ran": both cursor and last_run_at gone.
    #[test]
    fn reset_clears_cursor_state_completely() {
        let s = store();
        s.update_legacy("orders", "2024-06-01").unwrap();
        s.reset("orders", "pg/db").unwrap();
        let after = s.get("orders", "").unwrap();
        assert!(after.last_cursor_value.is_none());
        assert!(
            after.last_run_at.is_none(),
            "reset must clear last_run_at as well"
        );
    }

    /// #22 (0.9.x audit): `reset` left `export_progression` behind, so
    /// `rivet state progression` reported a stale committed boundary after
    /// `state show` was already empty. Reset must clear progression too.
    #[test]
    fn reset_clears_committed_progression() {
        let s = store();
        s.update_legacy("orders", "100").unwrap();
        s.record_committed_incremental("orders", "100", "run-1")
            .unwrap();
        // Other exports' progression must survive — reset is per-export.
        s.record_committed_incremental("users", "9", "run-u")
            .unwrap();

        s.reset("orders", "pg/db").unwrap();

        let p = s.get_progression("orders").unwrap();
        assert!(
            p.committed.is_none() && p.verified.is_none(),
            "reset must clear the export's committed/verified boundary"
        );
        assert!(
            s.get_progression("users").unwrap().committed.is_some(),
            "reset must not touch another export's progression"
        );
    }

    fn metric(s: &StateStore, mode: &str, key: Option<&str>, max: &str) {
        metric_with_descriptor(s, mode, key, max, None);
    }

    fn metric_with_descriptor(
        s: &StateStore,
        mode: &str,
        key: Option<&str>,
        max: &str,
        descriptor: Option<&str>,
    ) {
        s.execute(
            "INSERT INTO export_metrics \
             (export_name, run_at, duration_ms, total_rows, status, mode, chunk_key, cursor_max, \
              key_descriptor_json) \
             VALUES ('orders', '2026-09-11T00:00:00Z', 1, 1, 'success', ?1, ?2, ?3, ?4)",
            &[
                mode.into(),
                key.map(str::to_string).into(),
                max.into(),
                descriptor.map(str::to_string).into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn get_owned_accepts_the_identity_that_wrote_the_cursor() {
        let s = store();
        put(&s, "orders", "", "100", "id", "").unwrap();
        let c = s.get_owned("orders", "", "id").unwrap();
        assert_eq!(c.last_cursor_value.as_deref(), Some("100"));
        assert_eq!(c.cursor_column.as_deref(), Some("id"));
    }

    #[test]
    fn get_owned_refuses_a_cursor_written_for_another_column() {
        let s = store();
        put(&s, "orders", "", "3711169", "idvisit", "").unwrap();
        let e = s
            .get_owned("orders", "", "visit_last_action_time")
            .unwrap_err();
        assert_eq!(
            crate::error::classify_exit(&e),
            5,
            "a protective refusal exits 5"
        );
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_STATE_CURSOR_OWNER_MISMATCH")
        );
        let msg = format!("{e:#}");
        assert!(
            msg.contains("idvisit")
                && msg.contains("visit_last_action_time")
                && msg.contains("`rivet state reset -c <config> --export orders`"),
            "{msg}"
        );
        assert!(
            crate::error::codes::STATE_CURSOR_OWNER_MISMATCH
                .action
                .starts_with("`rivet state reset -c <config> --export <name>`"),
        );
    }

    #[test]
    fn update_keeps_the_recorded_column() {
        let s = store();
        put(&s, "orders", "", "1", "id", "").unwrap();
        s.update_legacy("orders", "2").unwrap();
        assert_eq!(
            s.get("orders", "").unwrap().cursor_column.as_deref(),
            Some("id")
        );
    }

    #[test]
    fn get_owned_without_a_cursor_value_never_refuses() {
        let s = store();
        anchor(&s, "orders", "", "r1", "").unwrap();
        assert!(s.get_owned("orders", "", "anything").is_ok());
    }

    #[test]
    fn legacy_row_is_owned_by_the_keyset_run_that_wrote_it() {
        let s = store();
        s.update_legacy("orders", "3711169").unwrap();
        metric(&s, "keyset", Some("idvisit"), "3711169");
        assert!(s.get_owned("orders", "", "idvisit").is_ok());
        assert!(s.get_owned("orders", "", "visit_last_action_time").is_err());
    }

    /// A 0.25.0 incremental run left no `cursor_column`, but its `export_metrics` row
    /// carries `key_descriptor_json = {"strategy":"incremental","key":<column>}` and the
    /// value it wrote: the cursor is attributed to that column, and a switched
    /// `cursor_column` is refused instead of comparing the new column against the old
    /// column's value (MT6 — the field bug on every 0.25.0 upgrade). RED against the
    /// keyset-only attribution.
    #[test]
    fn legacy_row_is_owned_by_the_incremental_run_that_wrote_it() {
        let s = store();
        s.update_legacy("orders", "3711169").unwrap();
        metric_with_descriptor(
            &s,
            "incremental",
            None,
            "3711169",
            Some(r#"{"strategy":"incremental","key":"idvisit","db_type":"int(10) unsigned"}"#),
        );
        assert!(s.get_owned("orders", "", "idvisit").is_ok());
        let err = s
            .get_owned("orders", "", "visit_last_action_time")
            .unwrap_err()
            .to_string();
        assert!(err.contains("written for `idvisit`"), "{err}");
        assert!(err.contains("state reset"), "{err}");
        // The descriptor names the primary column; a coalesce identity led by it is the
        // same cursor continuing, not a switch.
        assert!(
            s.get_owned("orders", "", "coalesce(idvisit,updated_at)")
                .is_ok()
        );
        assert!(
            s.get_owned("orders", "", "coalesce(updated_at,idvisit)")
                .is_err()
        );
        // Another value than the one the run wrote: not that run's cursor.
        let t = store();
        t.update_legacy("orders", "500").unwrap();
        metric_with_descriptor(
            &t,
            "incremental",
            None,
            "499",
            Some(r#"{"strategy":"incremental","key":"id"}"#),
        );
        assert!(t.get_owned("orders", "", "other").is_ok());
    }

    #[test]
    fn a_recorded_identity_is_matched_exactly_and_a_legacy_one_by_its_leading_column() {
        assert!(identity_matches("id", "id", false));
        assert!(!identity_matches("id", "coalesce(id,updated_at)", false));
        assert!(identity_matches("id", "coalesce(id,updated_at)", true));
        assert!(!identity_matches("id", "coalesce(idx,updated_at)", true));
        assert!(!identity_matches("id", "coalesce(updated_at,id)", true));
        assert_eq!(
            descriptor_cursor_column(r#"{"strategy":"incremental","key":"ts"}"#).as_deref(),
            Some("ts")
        );
        assert_eq!(
            descriptor_cursor_column(r#"{"strategy":"chunked","key":"id"}"#),
            None
        );
        assert_eq!(descriptor_cursor_column("not json"), None);
    }

    #[test]
    fn legacy_row_without_a_matching_keyset_run_is_not_refused() {
        let s = store();
        s.update_legacy("orders", "2026-09-11 10:00:00").unwrap();
        metric(&s, "keyset", Some("idvisit"), "3711169");
        assert!(s.get_owned("orders", "", "updated_at").is_ok());

        let t = store();
        t.update_legacy("orders", "500").unwrap();
        metric(&t, "incremental", None, "500");
        assert!(t.get_owned("orders", "", "anything").is_ok());
    }
}
