# Rivet — multi-export run with `--parallel-export-processes`.
# Each export gets its own card (header + run_id/status/mode/tuning/batch_size
# meta lines + live progress bar).  Cards stay fixed in place; on finish the
# bar is replaced in-place with the final metrics.

Output parallel-cards.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 780
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:$(dirname $PSQL_BIN):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=error"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# 4 chunked exports, one rivet child process each, one card per export"
Enter
Sleep 1200ms

Type "rivet run --config parallel-cards.yaml --parallel-export-processes"
Enter
Sleep 30s
