#!/usr/bin/env bash
# Regenerate every YAML fixture under cfg/. Idempotent. Fixtures are committed
# as plain files so reviewers can read them directly; this script exists to
# keep them in sync if axes need to grow.
#
# Naming: <group_letter><nn>_<short_label>.yaml
#   a = source-connection axis
#   b = TLS-mode axis
#   c = export mode / source-of-query / format
#   d = destination type
#   e = edge configuration (drift, quality, compression, row-group, meta)
#   f = multi-export / parallel / notifications
#   g = negative (should fail validate, with a useful error)
#
# Each fixture targets PG/MySQL on docker-compose's default ports. Group `d`
# destination configs that hit S3/GCS/Azure won't really connect — we only
# want to see the YAML-parse + early-validation behavior.
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
CFG="$ROOT/cfg"

w() {
  # w <relative-path-under-cfg> <<< 'yaml body'
  local path="$CFG/$1"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
}

# ── A. Source connection method × source_type ─────────────────────────────

w source/a01_pg_url.yaml <<'YAML'
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a01 }
YAML

w source/a02_pg_url_env.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a02 }
YAML

w source/a03_pg_url_file.yaml <<'YAML'
source:
  type: postgres
  url_file: ../../cfg_matrix/_pg_url.txt
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a03 }
YAML

w source/a04_pg_structured_password.yaml <<'YAML'
source:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a04 }
YAML

w source/a05_pg_structured_password_env.yaml <<'YAML'
source:
  type: postgres
  host: 127.0.0.1
  port: 5432
  user: rivet
  password_env: PG_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a05 }
YAML

w source/a06_my_url.yaml <<'YAML'
source:
  type: mysql
  url: "mysql://rivet:rivet@127.0.0.1:3306/rivet"
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a06 }
YAML

w source/a07_my_url_env.yaml <<'YAML'
source:
  type: mysql
  url_env: MY_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a07 }
YAML

w source/a08_my_url_file.yaml <<'YAML'
source:
  type: mysql
  url_file: ../../cfg_matrix/_my_url.txt
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a08 }
YAML

w source/a09_my_structured_password.yaml <<'YAML'
source:
  type: mysql
  host: 127.0.0.1
  port: 3306
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a09 }
YAML

w source/a10_my_structured_password_env.yaml <<'YAML'
source:
  type: mysql
  host: 127.0.0.1
  port: 3306
  user: rivet
  password_env: MY_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/a10 }
YAML

# ── B. TLS mode axis (PG + MySQL, every mode + invalid-cert escapes) ─────

w tls/b01_pg_tls_disable.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: disable }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b01 }
YAML

w tls/b02_pg_tls_require.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: require }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b02 }
YAML

w tls/b03_pg_tls_verify_ca.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-ca }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b03 }
YAML

w tls/b04_pg_tls_verify_full.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-full }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b04 }
YAML

w tls/b05_pg_tls_accept_invalid_certs.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: require, accept_invalid_certs: true }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b05 }
YAML

w tls/b06_my_tls_disable.yaml <<'YAML'
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: disable }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b06 }
YAML

w tls/b07_my_tls_require.yaml <<'YAML'
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: require }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b07 }
YAML

w tls/b08_my_tls_verify_ca.yaml <<'YAML'
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: verify-ca }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b08 }
YAML

w tls/b09_my_tls_verify_full.yaml <<'YAML'
source:
  type: mysql
  url_env: MY_URL
  tls: { mode: verify-full }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b09 }
YAML

w tls/b10_pg_tls_accept_invalid_hostnames.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tls: { mode: verify-full, accept_invalid_hostnames: true }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/b10 }
YAML

# ── C. Export mode × source of query × format ────────────────────────────

w export/c01_full_query_parquet.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c01 }
YAML

w export/c02_full_query_csv.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: csv
    destination: { type: local, path: ./out/c02 }
YAML

w export/c03_full_table_shortcut.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c03 }
YAML

w export/c04_full_query_file.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query_file: query_c04.sql
    mode: full
    format: parquet
    destination: { type: local, path: ./out/c04 }
YAML

w export/c05_incremental_parquet.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: id
    format: parquet
    destination: { type: local, path: ./out/c05 }
YAML

w export/c06_chunked_parquet.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/c06 }
YAML

w export/c07_chunked_with_checkpoint.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    chunk_checkpoint: true
    format: parquet
    destination: { type: local, path: ./out/c07 }
YAML

w export/c08_time_window_parquet.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: time_window
    time_column: created_at
    days_window: 7
    format: parquet
    destination: { type: local, path: ./out/c08 }
YAML

w export/c09_incremental_with_fallback.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: id
    format: parquet
    destination: { type: local, path: ./out/c09 }
YAML

w export/c10_chunked_with_chunk_count.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_count: 4
    format: parquet
    destination: { type: local, path: ./out/c10 }
YAML

w export/c11_chunked_by_days.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: created_at
    chunk_by_days: 1
    format: parquet
    destination: { type: local, path: ./out/c11 }
YAML

w export/c12_full_skip_empty.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE 1=0"
    mode: full
    skip_empty: true
    format: parquet
    destination: { type: local, path: ./out/c12 }
YAML

# Sibling query file for c04
cat > "$CFG/export/query_c04.sql" <<'SQL'
SELECT id, name FROM pa_audit
SQL

# ── D. Destination types ──────────────────────────────────────────────────

w destination/d01_dest_local.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/d01 }
YAML

w destination/d02_dest_stdout.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: stdout }
YAML

w destination/d03_dest_s3.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: rivet-cfg-matrix-test
      prefix: d03/
      region: us-east-1
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY
YAML

w destination/d04_dest_gcs.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: rivet-cfg-matrix-test
      prefix: d04/
      credentials_file: /tmp/gcs.json
YAML

w destination/d05_dest_azure.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination:
      type: azure
      account_name: rivetcfgmatrix
      bucket: container
      prefix: d05/
YAML

# ── E. Edge: drift / quality / compression / row-group / meta ────────────

w edge/e01_drift_warn.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: warn
    destination: { type: local, path: ./out/e01 }
YAML

w edge/e02_drift_continue.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: continue
    destination: { type: local, path: ./out/e02 }
YAML

w edge/e03_drift_fail.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    on_schema_drift: fail
    destination: { type: local, path: ./out/e03 }
YAML

w edge/e04_quality_row_count.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    quality:
      row_count_min: 1
      row_count_max: 1000
    destination: { type: local, path: ./out/e04 }
YAML

w edge/e05_quality_null_ratio_unique.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    quality:
      null_ratio_max: { name: 0.5 }
      unique_columns: [id]
    destination: { type: local, path: ./out/e05 }
YAML

w edge/e06_compression_profile_fast.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression_profile: fast
    destination: { type: local, path: ./out/e06 }
YAML

w edge/e07_compression_zstd_level.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: zstd
    compression_level: 9
    destination: { type: local, path: ./out/e07 }
YAML

w edge/e08_meta_columns_row_hash.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    meta_columns: { exported_at: true, row_hash: true }
    destination: { type: local, path: ./out/e08 }
YAML

w edge/e09_row_group_fixed_rows.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    parquet:
      row_group_strategy: fixed_rows
      row_group_rows: 500
    destination: { type: local, path: ./out/e09 }
YAML

w edge/e10_compression_profile_and_level.yaml <<'YAML'
# profile is supposed to take precedence over (compression + compression_level)
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: gzip
    compression_level: 3
    compression_profile: compact
    destination: { type: local, path: ./out/e10 }
YAML

# ── F. Multi-export / parallel / notifications ───────────────────────────

w multi/f01_two_exports.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f01_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f01_b }
YAML

w multi/f02_parallel_exports.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
parallel_exports: true
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f02_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f02_b }
YAML

w multi/f03_parallel_export_processes.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
parallel_export_processes: true
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f03_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f03_b }
YAML

w multi/f04_slack_webhook.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
notifications:
  slack:
    webhook_url: "https://hooks.slack.example/notreal"
    on: [failure, schema_change]
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f04 }
YAML

w multi/f05_slack_webhook_env.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK_URL
    on: [failure]
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/f05 }
YAML

# ── G. Negative: should fail with a clear error ──────────────────────────

w negative/g01_url_plus_url_env.yaml <<'YAML'
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
  url_env: PG_URL
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g01 }
YAML

w negative/g02_url_plus_structured.yaml <<'YAML'
source:
  type: postgres
  url: "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
  host: 127.0.0.1
  user: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g02 }
YAML

w negative/g03_password_plus_password_env.yaml <<'YAML'
source:
  type: postgres
  host: 127.0.0.1
  user: rivet
  password: rivet
  password_env: PG_PASSWORD
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g03 }
YAML

w negative/g04_query_plus_query_file.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    query_file: ../export/query_c04.sql
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g04 }
YAML

w negative/g05_query_plus_table.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    table: pa_audit
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g05 }
YAML

w negative/g06_missing_source.yaml <<'YAML'
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g06 }
YAML

w negative/g07_empty_exports.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports: []
YAML

w negative/g08_structured_missing_host.yaml <<'YAML'
source:
  type: postgres
  user: rivet
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g08 }
YAML

w negative/g09_structured_missing_user.yaml <<'YAML'
source:
  type: postgres
  host: 127.0.0.1
  password: rivet
  database: rivet
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g09 }
YAML

w negative/g10_unknown_top_level_key.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
mystery_key: 42
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g10 }
YAML

w negative/g11_unknown_export_key.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    nonsense_export_field: yes
    destination: { type: local, path: ./out/g11 }
YAML

w negative/g12_bad_export_mode.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: gibberish
    format: parquet
    destination: { type: local, path: ./out/g12 }
YAML

w negative/g13_bad_format.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: avro
    destination: { type: local, path: ./out/g13 }
YAML

w negative/g14_misplaced_tuning.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT 1"
    mode: full
    format: parquet
    batch_size: 100   # belongs under tuning:, not on the export root
    destination: { type: local, path: ./out/g14 }
YAML

w negative/g15_malformed_yaml.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL
exports:
  - name: pa_audit
YAML

w negative/g16_query_file_traversal.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query_file: ../../../../etc/passwd
    mode: full
    format: parquet
    destination: { type: local, path: ./out/g16 }
YAML

w negative/g17_incremental_no_cursor.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    format: parquet
    destination: { type: local, path: ./out/g17 }
YAML

# ── Edge: tuning subgraph + cursor modes + compression variants ───────────

w edge/e11_tuning_source_level.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  tuning:
    profile: fast
    batch_size: 500
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e11 }
YAML

w edge/e12_tuning_export_overrides.yaml <<'YAML'
# Export-level tuning takes precedence over source-level. Pin so a refactor
# that flips the merge precedence is caught.
source:
  type: postgres
  url_env: PG_URL
  tuning:
    profile: balanced
    batch_size: 1000
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e12 }
    tuning:
      batch_size: 100
YAML

w edge/e13_tuning_batch_memory.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e13 }
    tuning:
      batch_size_memory_mb: 16
YAML

w edge/e14_tuning_throttle.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e14 }
    tuning:
      throttle_ms: 5
YAML

w edge/e15_compression_lz4.yaml <<'YAML'
# lz4 is in the enum but was not exercised by the original sweep — pin so
# a downstream parquet-writer refactor that drops a codec is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: lz4
    destination: { type: local, path: ./out/e15 }
YAML

w edge/e16_compression_none.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    compression: none
    destination: { type: local, path: ./out/e16 }
YAML

w edge/e17_source_environment_production.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  environment: production
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e17 }
YAML

w edge/e18_source_environment_replica.yaml <<'YAML'
source:
  type: postgres
  url_env: PG_URL
  environment: replica
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/e18 }
YAML

w edge/e19_incremental_cursor_coalesce.yaml <<'YAML'
# `coalesce` mode is what `cursor_fallback_column` requires — pin the happy
# path so a refactor that drops the COALESCE rewrite is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: incremental
    cursor_column: id
    cursor_fallback_column: id
    incremental_cursor_mode: coalesce
    format: parquet
    destination: { type: local, path: ./out/e19 }
YAML

w edge/e20_max_file_size.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    max_file_size: "10MB"
    destination: { type: local, path: ./out/e20 }
YAML

w edge/e21_chunk_size_memory_mb.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size_memory_mb: 32
    format: parquet
    destination: { type: local, path: ./out/e21 }
YAML

# ── Export: MySQL × time_window — only PG was covered before ──────────────

w export/c13_time_window_mysql.yaml <<'YAML'
source: { type: mysql, url_env: MY_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: time_window
    time_column: created_at
    days_window: 7
    format: parquet
    destination: { type: local, path: ./out/c13 }
YAML

w export/c14_chunked_mysql.yaml <<'YAML'
source: { type: mysql, url_env: MY_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/c14 }
YAML

w negative/g18_chunked_no_column.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/g18 }
YAML

# Sidecar files used by url_file tests
echo "postgresql://rivet:rivet@127.0.0.1:5432/rivet" > "$ROOT/_pg_url.txt"
echo "mysql://rivet:rivet@127.0.0.1:3306/rivet"    > "$ROOT/_my_url.txt"

echo "Generated $(find "$CFG" -name '*.yaml' | wc -l | tr -d ' ') YAML fixtures under $CFG"
