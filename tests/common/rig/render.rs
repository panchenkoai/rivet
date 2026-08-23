//! RENDER — the pure half: builder state -> YAML string. Never touches the
//! filesystem (the offline goldens once caught a render that mkdir'd as a
//! side effect; purity here is load-bearing).

use super::*;

impl Rig {
    /// The rendered YAML — for suites that own their config-file lifetime.
    pub fn yaml(&self) -> String {
        self.render()
    }

    pub(crate) fn render(&self) -> String {
        let tables = match &self.query {
            Some(q) => format!("query: \"{q}\""),
            None if self.tables.len() == 1 => format!("table: {}", self.tables[0]),
            None => format!("tables: [{}]", self.tables.join(", ")),
        };
        let cdc_lines: Vec<String> = self
            .cdc_lines
            .iter()
            .map(|l| {
                if l == "__CKPT__" {
                    format!("checkpoint: \"{}\"", self.checkpoint().display())
                } else {
                    l.clone()
                }
            })
            .collect();
        let cdc = if cdc_lines.is_empty() {
            String::new()
        } else {
            format!("    cdc: {{ {} }}\n", cdc_lines.join(", "))
        };
        let extra: String = self
            .extra_lines
            .iter()
            .map(|l| format!("    {l}\n"))
            .collect();
        let source = if self.source_lines.is_empty() {
            match &self.url_env {
                Some(v) => format!("source: {{ type: {}, url_env: {v} }}", self.source_type),
                None => format!(
                    "source: {{ type: {}, url: \"{}\" }}",
                    self.source_type, self.source_url
                ),
            }
        } else {
            let extra: String = self
                .source_lines
                .iter()
                .map(|l| format!("  {l}\n"))
                .collect();
            match &self.url_env {
                Some(v) => format!(
                    "source:\n  type: {}\n  url_env: {v}\n{extra}",
                    self.source_type
                ),
                None => format!(
                    "source:\n  type: {}\n  url: \"{}\"\n{extra}",
                    self.source_type, self.source_url
                ),
            }
            .trim_end()
            .to_string()
        };
        // Secondary exports (see `Rig::also_export`) each get their OWN
        // destination, which is what the multi-export configs under test
        // actually declare — per-export failure isolation is only observable
        // when the outputs are separable.
        let secondaries: String = self
            .extra_exports
            .iter()
            .map(|e| {
                let lines: String = e.lines.iter().map(|l| format!("    {l}\n")).collect();
                let subject = match &e.table {
                    Some(t) => format!("table: {t}"),
                    None => format!("query: \"{}\"", e.query),
                };
                let cdc = if e.cdc_lines.is_empty() {
                    String::new()
                } else {
                    format!("    cdc: {{ {} }}\n", e.cdc_lines.join(", "))
                };
                format!(
                    "  - name: {n}\n    {subject}\n    mode: {m}\n    format: {f}\n{cdc}{lines}    destination: {o}\n",
                    n = e.name,
                    m = e.mode,
                    f = self.format,
                    o = self.dest_yaml(&e.name),
                )
            })
            .collect();
        let yaml = format!(
            "{source}\nexports:\n  - name: {name}\n    {tables}\n    mode: {mode}\n    format: {fmt}\n{cdc}{extra}    destination: {out}\n{secondaries}",
            name = self.name,
            tables = tables,
            mode = self.mode,
            fmt = self.format,
            out = self.dest_yaml(&self.name.clone()),
        );
        yaml
    }

    /// The destination YAML for `export` — S3 when [`Rig::dest_s3`] was called,
    /// the local tempdir otherwise. ONE renderer for both, so the primary and
    /// secondary exports cannot drift apart (they were two `format!`s before).
    pub(crate) fn dest_yaml(&self, export: &str) -> String {
        if self.dest_stdout {
            return "{ type: stdout }".to_string();
        }
        match &self.cloud_dest {
            Some(CloudDest::S3 {
                bucket,
                prefix,
                endpoint,
            }) => format!(
                "{{ type: s3, bucket: {bucket}, prefix: \"{prefix}/{export}/\", region: us-east-1, \
                 endpoint: \"{endpoint}\", access_key_env: RIVET_TEST_MINIO_AK, \
                 secret_key_env: RIVET_TEST_MINIO_SK }}"
            ),
            Some(CloudDest::Gcs {
                bucket,
                prefix,
                endpoint,
            }) => format!(
                "{{ type: gcs, bucket: {bucket}, prefix: \"{prefix}/{export}/\", \
                 endpoint: \"{endpoint}\", allow_anonymous: true }}"
            ),
            Some(CloudDest::Azure { container, prefix }) => format!(
                "{{ type: azure, bucket: {container}, prefix: \"{prefix}/{export}/\", \
                 account_name: {act}, account_key_env: RIVET_TEST_AZURITE_KEY, \
                 endpoint: \"{ep}\" }}",
                act = crate::common::env::AZURITE_ACCOUNT,
                ep = crate::common::env::AZURITE_ENDPOINT,
            ),
            None => format!(
                "{{ type: local, path: \"{}\" }}",
                self.out_dir_for(export).display()
            ),
        }
    }
}
