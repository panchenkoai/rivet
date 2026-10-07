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
    /// The config's own `parallel_export_processes: true`.
    config_asks_children: bool,
}

/// Why `rivet run` keeps a `partition_by` set in its own process.
const PARTITIONED_RUNS_IN_PROCESS: &str = "partition_by: --parallel-export-processes is disabled with partitioned exports \
     (child processes re-load the config and can't see synthesised partitions); \
     running in-process";

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
        Ok(Self {
            declared,
            config_asks_children: config.parallel_export_processes,
        })
    }

    /// Whether a selected export is a `partition_by` export.
    fn is_partitioned(&self) -> bool {
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

    /// Whether `--parallel-export-processes` (`cli`) or the config asks for child processes.
    fn asks_children(&self, cli: bool) -> bool {
        cli || self.config_asks_children
    }

    /// The declared exports for the child processes `cli` or the config asks for, each of which resolves its own run set; `Err` is why the set runs in-process instead.
    pub(super) fn child_processes(
        &self,
        cli: bool,
    ) -> std::result::Result<Option<(ChildProcessesOk, Vec<&ExportConfig>)>, String> {
        if !self.asks_children(cli) {
            return Ok(None);
        }
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
            None => Ok(Some((ChildProcessesOk(()), self.declared.iter().collect()))),
        }
    }

    /// `rivet run`'s narrower rule: it expands `partition_by` itself, so children get only an unpartitioned set of more than one export.
    pub(super) fn run_child_processes(
        &self,
        cli: bool,
    ) -> std::result::Result<Option<ChildProcessesOk>, String> {
        if self.asks_children(cli) && self.is_partitioned() {
            return Err(PARTITIONED_RUNS_IN_PROCESS.to_string());
        }
        Ok(self
            .child_processes(cli)?
            .filter(|(_, declared)| declared.len() > 1)
            .map(|(ok, _)| ok))
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

    /// `n` plain local exports `t1..tn`, under the top-level `top` lines.
    fn plain(n: usize, top: &str) -> Config {
        let exports: String = (1..=n)
            .map(|i| {
                format!(
                    "  - name: t{i}\n    table: t{i}\n    mode: full\n    format: parquet\n\
                     \x20   destination: {{ type: local, path: ./out/t{i} }}\n"
                )
            })
            .collect();
        Config::from_yaml(&format!(
            "source: {{ type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }}\n{top}exports:\n{exports}"
        ))
        .expect("the plain config loads")
    }

    /// The names `rivet apply` would hand to children, `None` when the set runs in-process without a refusal.
    fn apply_children(cfg: &Config, cli: bool) -> Option<Vec<String>> {
        let set = RunSet::select(cfg, None).unwrap();
        let children = set.child_processes(cli).expect("no export refuses");
        children.map(|(_, declared)| names(declared).into_iter().map(String::from).collect())
    }

    /// Whether `rivet run` would hand the whole config to children.
    fn run_children(cfg: &Config, cli: bool) -> bool {
        let set = RunSet::select(cfg, None).unwrap();
        set.run_child_processes(cli)
            .expect("no export refuses")
            .is_some()
    }

    /// Children run only when the flag or the config's `parallel_export_processes` asks, either one alone.
    #[test]
    fn children_run_when_the_flag_or_the_config_asks() {
        let (quiet, asking) = (plain(2, ""), plain(2, "parallel_export_processes: true\n"));
        let both = Some(vec!["t1".to_string(), "t2".to_string()]);
        assert_eq!(apply_children(&quiet, false), None);
        assert_eq!(apply_children(&quiet, true), both);
        assert_eq!(apply_children(&asking, false), both);
        assert_eq!(apply_children(&asking, true), both);
        assert!(!run_children(&quiet, false));
        assert!(run_children(&quiet, true));
        assert!(run_children(&asking, false));
        assert!(run_children(&asking, true));
    }

    /// `rivet run` forks only for more than one export; `rivet apply` hands even a single one to a child.
    #[test]
    fn run_keeps_a_single_export_in_process_and_apply_does_not() {
        assert!(!run_children(&plain(1, ""), true));
        assert!(run_children(&plain(2, ""), true));
        assert!(run_children(&plain(3, ""), true));
        assert_eq!(apply_children(&plain(1, ""), true), Some(vec!["t1".into()]));
        let named = RunSet::select(&plain(3, ""), Some("t2")).unwrap();
        assert!(named.run_child_processes(true).unwrap().is_none());
    }

    /// `rivet run` refuses children for a `partition_by` set in its own words, and only when they were asked for; `rivet apply` sends the declared export.
    #[test]
    fn run_keeps_a_partitioned_set_in_process_and_says_why() {
        let cfg = config();
        let events = RunSet::select(&cfg, Some("events")).unwrap();
        assert_eq!(
            events.run_child_processes(true).unwrap_err(),
            "partition_by: --parallel-export-processes is disabled with partitioned exports \
             (child processes re-load the config and can't see synthesised partitions); \
             running in-process"
        );
        assert!(events.run_child_processes(false).unwrap().is_none());
        let (_, children) = events
            .child_processes(true)
            .unwrap()
            .expect("a partitioned export goes to a child, which expands it");
        assert_eq!(names(children), ["events"]);
        let whole = RunSet::select(&cfg, None).unwrap();
        assert!(
            whole
                .run_child_processes(true)
                .unwrap_err()
                .starts_with("partition_by:"),
            "a partitioned set names its partitions before its stdout export"
        );
    }

    /// A set with a stdout export never goes to children: the refusal names the export and the fallback, and is silent unless children were asked for.
    #[test]
    fn child_processes_take_the_declared_exports_unless_one_writes_to_stdout() {
        let cfg = config();
        let why = "destination: stdout: --parallel-export-processes is disabled when an export writes \
             to stdout (export 'feed': a child process's stdout is its event channel to this \
             process, so the data would never reach this process's stdout); running in-process";
        let whole = RunSet::select(&cfg, None).unwrap();
        assert_eq!(whole.child_processes(true).unwrap_err(), why);
        assert!(whole.child_processes(false).unwrap().is_none());
        let feed = RunSet::select(&cfg, Some("feed")).unwrap();
        assert_eq!(feed.run_child_processes(true).unwrap_err(), why);
        assert!(feed.run_child_processes(false).unwrap().is_none());
    }
}
