//! MATERIALIZE — where the rendered YAML meets the filesystem: config_path /
//! config_in / amend / replace, plus the hand-edit guard. The invariant this
//! module owns: the FILE and the BUILDER never silently disagree (bughunt:
//! three resume tests ran an un-patched config while looking patched).

use super::*;

impl Rig {
    /// Materialize this rig's config into a CALLER-owned directory and return
    /// the path — for scenarios where the config must OUTLIVE the rig value.
    ///
    /// This is the sanctioned answer to the temporary-rig-drop trap: a
    /// `rig.config_path()` handed out of a helper dies with the rig's tempdir
    /// (live_cdc documented the trap and kept `write_config(d, &rig.yaml())`
    /// round-trips as the workaround; live_cdc_mssql hit it live during the
    /// migration). Destinations are still pre-created exactly as
    /// [`Rig::config_path`] does, so the two materializations cannot drift.
    pub fn config_in(&self, dir: &std::path::Path) -> PathBuf {
        if self.dest_precreate {
            std::fs::create_dir_all(self.out_dir()).unwrap();
        }
        for e in &self.extra_exports {
            std::fs::create_dir_all(self.out_dir_for(&e.name)).unwrap();
        }
        let cfg = dir.join("rig.yaml");
        self.write_guarded(&cfg, &self.render());
        // Track the caller-owned copy so amend/replace re-render it too —
        // otherwise a sanctioned mutation updates only the rig-dir config and
        // the caller's path silently keeps executing the OLD knobs (r2
        // bughunt: exactly the measured-nothing class the guard exists for).
        self.materialized_copies.borrow_mut().push(cfg.clone());
        cfg
    }

    /// Write `rendered` to `path`, refusing to clobber a HAND-EDITED file
    /// (one whose current content matches neither the new render nor any
    /// prior rig render — i.e. someone else wrote it).
    fn write_guarded(&self, path: &std::path::Path, rendered: &str) {
        if let Ok(existing) = std::fs::read_to_string(path)
            && existing != rendered
            && !self.past_renders.borrow().iter().any(|r| r == &existing)
        {
            panic!(
                "rig config at {} was edited outside the rig; every rig \
                 materialization re-renders it, so the edit would be silently \
                 discarded. Mutate the rig instead (amend_export_lines / \
                 replace_export_line), or run the edited file via run_rivet.",
                path.display()
            );
        }
        std::fs::write(path, rendered).unwrap();
        let mut past = self.past_renders.borrow_mut();
        if !past.iter().any(|r| r == rendered) {
            past.push(rendered.to_string());
        }
    }

    pub fn config_path(&self) -> PathBuf {
        // Materialization point: the ONLY place the rig touches the
        // filesystem (yaml()/render() stay pure — the offline goldens were
        // mkdir-ing /tmp/o as a side effect of rendering a string).
        if self.dest_precreate {
            std::fs::create_dir_all(self.out_dir()).unwrap();
        }
        for e in &self.extra_exports {
            std::fs::create_dir_all(self.out_dir_for(&e.name)).unwrap();
        }
        let cfg = self.dir.path().join("rig.yaml");
        // Every run helper re-materializes through here, so a caller who
        // HAND-EDITED the file would have the patch silently clobbered and the
        // test would run the un-patched config while looking patched (bughunt:
        // three chunked-recovery resume tests measured nothing on their
        // changed-parallelism leg exactly this way). write_guarded refuses
        // loudly; the sanctioned mutations are amend_export_lines /
        // replace_export_line.
        self.write_guarded(&cfg, &self.render());
        cfg
    }

    pub fn amend_export_lines(&mut self, lines: &[&str]) -> PathBuf {
        for l in lines {
            self.extra_lines.push((*l).to_string());
        }
        self.rerender_all_materializations()
    }

    /// Re-render the rig-dir config AND every caller-owned `config_in` copy —
    /// a mutation that updated only one of them would leave the other
    /// silently executing yesterday's knobs.
    fn rerender_all_materializations(&self) -> PathBuf {
        let rendered = self.render();
        let path = self.dir.path().join("rig.yaml");
        self.write_guarded(&path, &rendered);
        for copy in self.materialized_copies.borrow().iter() {
            self.write_guarded(copy, &rendered);
        }
        path
    }

    /// Re-render THIS rig's config with extra export lines appended, over the
    /// SAME config path (and therefore the same adjacent `.rivet_state.db`).
    ///
    /// For two-phase fixtures whose subject is a SECOND run under a CHANGED
    /// config against the FIRST run's state — e.g. gremlin G2: run 1 advances an
    /// incremental cursor with no quality gate, run 2 adds `row_count_min` and
    /// must see the gate fire on the exhausted cursor. Without this the test
    /// hand-writes a second YAML into the rig's dir (`write_config`), which is
    /// the per-file config builder the rig exists to replace.
    /// Replace the first export line starting with `prefix` — for two-phase
    /// fixtures whose SECOND run must change a knob (`parallel: 1` -> `2`)
    /// over the SAME config path and state DB. Panics if no line matches:
    /// silently not-replacing is how a resume leg measures nothing.
    pub fn replace_export_line(&mut self, prefix: &str, new_line: &str) -> PathBuf {
        let slot = self
            .extra_lines
            .iter_mut()
            .find(|l| l.starts_with(prefix))
            .unwrap_or_else(|| {
                panic!("replace_export_line: no export line starts with {prefix:?}")
            });
        *slot = new_line.to_string();
        self.rerender_all_materializations()
    }

    pub fn checkpoint(&self) -> PathBuf {
        self.ckpt_override
            .clone()
            .unwrap_or_else(|| self.dir.path().join("cdc.ckpt"))
    }

    /// Materialized config path — for bespoke invocations (`validate`,
    /// custom envs) the rig doesn't wrap.
    /// The export name this rig rendered into the config — the key
    /// `export_metrics` rows are stored under, so a test can read the run's own
    /// metrics without re-deriving the name it did not choose.
    pub fn export_name(&self) -> &str {
        &self.name
    }
}
