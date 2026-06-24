# Rivet — parallel exports on the SAME table: a full snapshot AND a CDC stream of
# content_items run side by side under `--parallel-exports`, each its own card
# (header + run_id/status/mode + live progress, replaced in-place with final
# metrics on finish). Batch and CDC of one table, concurrently.
#
# Fixture (render.sh, fixture_cdc_parallel_setup): writes parallel.yaml (a
# mode: full + a mode: cdc export, both on content_items) and seeds the CDC
# checkpoint + a backlog so the cdc arm drains and exits.

Output cdc-parallel.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 760
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=error"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# one table, two exports: a full snapshot and a CDC stream, in parallel"
Enter
Sleep 1500ms

Type "sed -n '/exports:/,$p' parallel.yaml"
Enter
Sleep 3s

Type "rivet run --parallel-exports -c parallel.yaml"
Enter
Sleep 10s
