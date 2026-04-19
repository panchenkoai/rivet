# Rivet — machine-readable discovery artifact: ranked cursor/chunk
# candidates, row estimates, on-disk sizes, coalesce-fallback hints.

Output discover-artifact.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 640
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:$(dirname $PSQL_BIN):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "rivet init --source-env DATABASE_URL --schema public --discover -o discovery.json"
Enter
Sleep 3500ms

Type `jq '.tables[] | {table, rows: .row_estimate, mode: .suggested_mode, cursor: (.cursor_candidates[0].column // "-"), fallback: .suggested_cursor_fallback_column}' discovery.json`
Enter
Sleep 4s
