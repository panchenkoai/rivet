# Rivet — terminal progress bar during a chunked export.
# Fixture: rivet_gif.events with 50,000 rows, chunk_size 5,000 -> 10 chunks.
# Single worker (parallel: 1) so the bar advances smoothly.

Output chunked-progress.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 560
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:$(dirname $PSQL_BIN):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=info"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# Chunked export (50k rows / 10 chunks) — per-chunk log lines + end-of-run summary"
Enter
Sleep 1500ms

Type `rivet run --config chunked-progress.yaml --validate 2>&1 | grep -E 'chunk [0-9]+/10|chunks completed|---|run_id|status|rows|files|bytes|duration|peak|validated'`
Enter
Sleep 9s
