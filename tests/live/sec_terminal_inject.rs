//! SEC-RED terminal-inject cluster.
//!
//! V9 (MEDIUM): `src/pipeline/parent_ui.rs` (the `run_ui` renderer, line ~392,
//! and the parallel-children child-stderr capture) re-emits child / IPC text —
//! including attacker-controlled DB error strings — to the operator terminal
//! with NO control-character stripping. A malicious (or compromised) source DB
//! can therefore inject ANSI / OSC escape sequences (window-title rewrite,
//! screen clear, hyperlink spoof, cursor games) straight into the operator's
//! terminal whenever an export fails and rivet renders the error card.
//!
//! Exploit chain (verified by reading code):
//!   source error  ->  `redact::redact_error` (strips URL passwords ONLY,
//!   never control bytes)  ->  `RunSummary.error_message`  ->  IPC `Finished`
//!   ->  `parent_ui::render_final_line(error_message)` (returns it verbatim via
//!   `strip_chunked_recovery_hint`)  ->  `CardState.final_line`  ->
//!   `compact_line` -> `clamp_line` (truncates by char count, does NOT strip
//!   control bytes)  ->  `redraw()` / `flush_finished_cards` -> operator stderr.
//!
//! ## Why these are LIVE tests through the binary seam (not unit tests)
//!
//! The whole V9 data path (`render_final_line`, `compact_line`, `CardState`,
//! `strip_chunked_recovery_hint`, `clamp_line`) lives behind `pub(crate)`
//! visibility (`pub(crate) mod parent_ui;`), so NONE of it is reachable from an
//! external integration-test crate. A unit test for the format seam would have
//! to be appended to `parent_ui.rs`'s in-module `#[cfg(test)]` block — i.e.
//! production code, which this phase must not touch. The only stable public
//! seam that exercises the vulnerable renderer is the `rivet` binary itself:
//! a failing export prints its `error_message` through `run_ui`. We drive a
//! real failure whose DB error text carries escape bytes and assert the bytes
//! never reach the operator's stderr stream.
//!
//! Both tests assert the SECURE behavior and are expected to FAIL against the
//! current (vulnerable) code, which emits the raw escape bytes unmodified.

use crate::common::*;

/// C0/C1 control bytes that must never reach an operator's terminal raw.
/// We allow only TAB (0x09), LF (0x0a) and CR (0x0d) as legitimate layout
/// whitespace the renderer already relies on; everything else in the C0 range
/// (0x00..=0x1f) and the DEL/C1 range (0x7f..=0x9f) is a terminal-control byte
/// an attacker can weaponise (ESC 0x1b for CSI/OSC, BEL 0x07, etc.).
fn dangerous_control_bytes(bytes: &[u8]) -> Vec<char> {
    // Scan DECODED chars, not raw bytes: a byte scan would false-flag the UTF-8
    // continuation bytes (0x80..=0xBF) of the renderer's legitimate multi-byte
    // glyphs (✓ ✗ ── — …) and any non-ASCII data. Mirrors the production
    // sanitize_terminal contract: C0 except TAB/LF, DEL, and C1 are dangerous.
    String::from_utf8_lossy(bytes)
        .chars()
        .filter(|&c| {
            let cp = c as u32;
            (cp <= 0x1f && c != '\t' && c != '\n') || cp == 0x7f || (0x80..=0x9f).contains(&cp)
        })
        .collect()
}

/// A source query whose RUNTIME failure echoes attacker-controlled escape
/// bytes back in the database's own error message.
///
/// The bytes are produced *by the database* with `chr(27)` (ESC) and `chr(7)`
/// (BEL) and assembled into:
///   * `ESC ] 0 ; pwned BEL`  — OSC 0: rewrite the terminal window/tab title
///   * `ESC [ 2 J`            — CSI 2J: clear the entire screen
///
/// Casting that string to `int` fails, and Postgres reports
/// `invalid input syntax for type integer: "<ESC>]0;pwned<BEL><ESC>[2Jboom"`,
/// echoing the escape bytes verbatim into the error text. Generating the bytes
/// DB-side keeps the YAML config pure ASCII, so the config parser cannot reject
/// it for embedded control characters before the query ever runs — the leak
/// then arrives strictly through the DB-error -> `run_ui` render path that V9
/// flags. The payload sits at the front of the cast value so it survives the
/// renderer's width clamp (`clamp_line`, 60..=100 chars).
fn ansi_injection_query(table: &str) -> String {
    // chr(27)=ESC, chr(7)=BEL. Built entirely in SQL so the YAML stays ASCII.
    format!("SELECT (chr(27) || ']0;pwned' || chr(7) || chr(27) || '[2Jboom')::int FROM {table}")
}

/// SEC-RED V9: a failing single export must NOT leak attacker-controlled ANSI/
/// OSC escape bytes (from the source DB error text) to the operator terminal.
///
/// Single-export `rivet run` still routes its progress/error rendering through
/// `parent_ui::run_ui` (see `pipeline/run.rs`: "Always route through
/// `parent_ui`"), so this exercises the exact V9 renderer. The export fails
/// because the query casts a DB-built escape-sequence string to `int`; Postgres
/// returns `invalid input syntax for type integer: "<payload>"`, rivet redacts
/// only secrets (not control bytes) and renders the error card to stderr.
///
/// SECURE assertion: the captured stderr carries NO raw dangerous control
/// bytes. Expected to FAIL today (raw ESC/BEL reach the terminal); passes once
/// `parent_ui` sanitises terminal-bound text.
#[test]
#[ignore = "live: docker postgres"]
fn sec_db_error_escapes_stripped_from_terminal() {
    // SEC-RED V9: ANSI/OSC escape injection via unsanitised DB error -> terminal.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(3);
    let export_name = unique_name("sec_v9_single");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();

    // The query is pure ASCII; Postgres builds the escape bytes itself (chr(27)
    // / chr(7)) and echoes them in the cast-failure error, so the config parser
    // never sees a raw control byte and the leak arrives strictly via the
    // DB-error -> run_ui render path.
    let query = ansi_injection_query(table.name());
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\n\
         exports:\n\
         \x20 - name: {export_name}\n\
         \x20   query: \"{query}\"\n\
         \x20   mode: full\n\
         \x20   format: parquet\n\
         \x20   destination: {{type: local, path: {dir}}}\n\
         \x20   tuning: {{max_retries: 0}}\n",
        dir = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = run_rivet_export(&cfg, &export_name);

    // The export must fail (the column does not exist) — this is the trigger
    // for the error-card render path. If it somehow succeeded the vuln would be
    // unexercised, so treat that as an inconclusive test failure.
    assert!(
        !run.status.success(),
        "expected the bogus-column export to fail so the error card renders"
    );

    // The renderer talks to the operator over stderr.
    let stderr = run.stderr;
    let leaked = dangerous_control_bytes(&stderr);
    assert!(
        leaked.is_empty(),
        "SEC-RED V9: source-DB error text leaked {} raw control byte(s) {:02x?} to \
         the operator terminal via parent_ui; ANSI/OSC escapes must be stripped/\
         escaped before render. stderr (lossy):\n{}",
        leaked.len(),
        leaked,
        String::from_utf8_lossy(&stderr),
    );
}

/// SEC-RED V9: the `--parallel-export-processes` path (the exact site the audit
/// names: parent UI + parallel-children child-stderr capture) must also strip
/// attacker-controlled escape bytes before re-emitting child/IPC error text to
/// the operator terminal.
///
/// Same injection as above, but driven through the process-mode renderer so the
/// child's failure travels over IPC to the parent `run_ui` before reaching
/// stderr — the precise data flow flagged in `parent_ui.rs`.
///
/// SECURE assertion: no raw dangerous control bytes reach stderr. Expected to
/// FAIL today; passes once the parent UI / child-stderr capture sanitises
/// terminal-bound text.
#[test]
#[ignore = "live: docker postgres"]
fn sec_parallel_db_error_escapes_stripped() {
    // SEC-RED V9: ANSI/OSC escape injection via unsanitised child/IPC error -> terminal.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(3);
    let export_name = unique_name("sec_v9_parallel");
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();

    let query = ansi_injection_query(table.name());
    let yaml = format!(
        "source: {{type: postgres, url: \"{POSTGRES_URL}\"}}\n\
         exports:\n\
         \x20 - name: {export_name}\n\
         \x20   query: \"{query}\"\n\
         \x20   mode: full\n\
         \x20   format: parquet\n\
         \x20   destination: {{type: local, path: {dir}}}\n\
         \x20   tuning: {{max_retries: 0}}\n",
        dir = out.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = run_rivet(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export_name,
        "--parallel-export-processes",
    ]);

    assert!(
        !run.status.success(),
        "expected the bogus-column export to fail so the error card renders"
    );

    let stderr = run.stderr;
    let leaked = dangerous_control_bytes(&stderr);
    assert!(
        leaked.is_empty(),
        "SEC-RED V9: child/IPC error text leaked {} raw control byte(s) {:02x?} to \
         the operator terminal via the parallel-export-processes renderer; ANSI/OSC \
         escapes must be stripped/escaped before render. stderr (lossy):\n{}",
        leaked.len(),
        leaked,
        String::from_utf8_lossy(&stderr),
    );
}
