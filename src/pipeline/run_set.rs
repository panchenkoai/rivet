//! The exports one invocation runs, resolved once for every entry point (`run`, `apply`, `--pool`).

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, DestinationType, ExportConfig, SourceConfig};
use crate::error::Result;

use super::partition_expand;

/// Proof that a run set may go to `--parallel-export-processes` children; only [`RunSet::child_processes`] makes one.
#[derive(Clone, Copy, Debug)]
pub(super) struct ChildProcessesOk(());

/// The exports an invocation was asked for: one named export, or the whole config minus the CDC backfill recipes.
pub(super) struct RunSet {
    declared: Vec<ExportConfig>,
}

impl RunSet {
    /// Select `export_name`, or every export a whole-config invocation runs itself.
    pub(super) fn select(config: &Config, export_name: Option<&str>) -> Result<Self> {
        let declared = match export_name {
            Some(name) => vec![
                config
                    .exports
                    .iter()
                    .find(|e| e.name == name)
                    .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?
                    .clone(),
            ],
            None => {
                let recipes = backfill_recipes_to_skip(&config.exports);
                crate::config::without_backfill_recipes(&config.exports, &recipes)
                    .into_iter()
                    .cloned()
                    .collect()
            }
        };
        Ok(Self { declared })
    }

    /// Whether a selected export is a `partition_by` export.
    pub(super) fn is_partitioned(&self) -> bool {
        partition_expand::any_partitioned(&self.declared.iter().collect::<Vec<_>>())
    }

    /// The exports to run in this process: each `partition_by` export replaced by one export per bucket.
    pub(super) fn in_process(
        &self,
        source: &SourceConfig,
        config_dir: &Path,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<ExportConfig>> {
        let declared: Vec<&ExportConfig> = self.declared.iter().collect();
        partition_expand::expand_partitioned_exports(&declared, source, config_dir, params)
    }

    /// The declared exports for child processes, each of which resolves its own run set; `Err` is why children cannot run them.
    pub(super) fn child_processes(
        &self,
    ) -> std::result::Result<(ChildProcessesOk, Vec<&ExportConfig>), String> {
        match self
            .declared
            .iter()
            .find(|e| e.destination.destination_type == DestinationType::Stdout)
        {
            Some(e) => Err(format!(
                "destination: stdout: --parallel-export-processes is disabled when an export writes \
                 to stdout (export '{}': a child process's stdout is its event channel to this \
                 process, so the data would never reach this process's stdout); running in-process",
                e.name
            )),
            None => Ok((ChildProcessesOk(()), self.declared.iter().collect())),
        }
    }
}

/// The names a whole-config invocation must not run itself: each is a `mode: cdc` export's `backfill:` recipe.
fn backfill_recipes_to_skip(exports: &[ExportConfig]) -> std::collections::HashSet<String> {
    let recipes = crate::config::backfill_recipe_names(exports);
    for e in exports.iter().filter(|e| recipes.contains(&e.name)) {
        log::info!(
            "export '{}': skipped — it is the backfill recipe of a `mode: cdc` export, \
             which runs it after the anchor (run it alone with `-e {}` to export it on \
             its own)",
            e.name,
            e.name
        );
    }
    recipes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config of `orders` (CDC, backfilled by `orders_base`), `orders_base`, a partitioned `events` and a stdout `feed`.
    fn config() -> Config {
        Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: orders\n    mode: cdc\n    table: orders\n    format: parquet\n\
             \x20   cdc: { checkpoint: /tmp/ck, backfill: [orders_base] }\n\
             \x20   destination: { type: local, path: ./out/orders }\n\
             \x20 - name: orders_base\n    table: orders\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/orders_base }\n\
             \x20 - name: events\n    table: events\n    mode: full\n    format: parquet\n\
             \x20   partition_by: created_at\n\
             \x20   destination: { type: local, path: \"./out/events/{partition}\" }\n\
             \x20 - name: feed\n    table: feed\n    mode: full\n    format: csv\n\
             \x20   destination: { type: stdout }\n",
        )
        .expect("the fixture config loads")
    }

    /// The names of `exports`, in order.
    fn names<'a>(exports: impl IntoIterator<Item = &'a ExportConfig>) -> Vec<&'a str> {
        exports.into_iter().map(|e| e.name.as_str()).collect()
    }

    /// A whole-config set drops the backfill recipe; a named one is exactly that export, recipe or not.
    #[test]
    fn a_whole_config_set_skips_backfill_recipes_and_a_named_one_does_not() {
        let cfg = config();
        let whole = RunSet::select(&cfg, None).unwrap();
        assert_eq!(names(&whole.declared), ["orders", "events", "feed"]);
        let named = RunSet::select(&cfg, Some("orders_base")).unwrap();
        assert_eq!(names(&named.declared), ["orders_base"]);
        let missing = RunSet::select(&cfg, Some("nope")).err().unwrap();
        assert_eq!(missing.to_string(), "export 'nope' not found in config");
    }

    /// `is_partitioned` reports a `partition_by` export in the selection, and only there.
    #[test]
    fn is_partitioned_reads_the_selection() {
        let cfg = config();
        assert!(RunSet::select(&cfg, None).unwrap().is_partitioned());
        assert!(
            RunSet::select(&cfg, Some("events"))
                .unwrap()
                .is_partitioned()
        );
        assert!(!RunSet::select(&cfg, Some("feed")).unwrap().is_partitioned());
    }

    /// With nothing to expand, the in-process set is the selection and no source is contacted.
    #[test]
    fn an_unpartitioned_set_runs_as_declared() {
        let cfg = config();
        let set = RunSet::select(&cfg, Some("orders_base")).unwrap();
        let exports = set.in_process(&cfg.source, Path::new("."), None).unwrap();
        assert_eq!(names(&exports), ["orders_base"]);
    }

    /// A `partition_by` export is expanded from the source, so an unreachable source fails the set.
    #[test]
    fn a_partitioned_set_is_expanded_from_the_source() {
        let cfg = config();
        let set = RunSet::select(&cfg, Some("events")).unwrap();
        assert!(set.in_process(&cfg.source, Path::new("."), None).is_err());
    }

    /// Children get the declared exports, unless one writes to stdout: the refusal names it and the fallback.
    #[test]
    fn child_processes_take_the_declared_exports_unless_one_writes_to_stdout() {
        let cfg = config();
        let events = RunSet::select(&cfg, Some("events")).unwrap();
        let (_, children) = events
            .child_processes()
            .expect("a partitioned export goes to a child, which expands it");
        assert_eq!(names(children), ["events"]);
        let why = RunSet::select(&cfg, None)
            .unwrap()
            .child_processes()
            .expect_err("`feed` writes to stdout");
        assert_eq!(
            why,
            "destination: stdout: --parallel-export-processes is disabled when an export writes \
             to stdout (export 'feed': a child process's stdout is its event channel to this \
             process, so the data would never reach this process's stdout); running in-process"
        );
    }
}
