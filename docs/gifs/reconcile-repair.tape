# Rivet — chunked export + reconcile + targeted repair.
# Requires:
#   - rivet_gif.events fixture (10,000 rows)
#   - ./chunked.yaml config in the workdir
# Both are prepared by docs/gifs/render.sh before vhs is invoked.

Output reconcile-repair.gif

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
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# 1. Chunked export with checkpoint — 4 chunks of 2,500 rows"
Enter
Sleep 1500ms

Type "rivet run --config chunked.yaml --validate 2>&1 | tail -n 20"
Enter
Sleep 4s

Type "# 2. Reconcile re-counts every partition on the source"
Enter
Sleep 1500ms

Type "rivet reconcile --config chunked.yaml --export events"
Enter
Sleep 3s

Type "# 3. Simulate drift: delete 500 rows from chunk #2 on the source"
Enter
Sleep 1500ms

Type `psql "$DATABASE_URL" -c "DELETE FROM rivet_gif.events WHERE id BETWEEN 2501 AND 3000" -q`
Enter
Sleep 1500ms

Type "rivet reconcile --config chunked.yaml --export events"
Enter
Sleep 3s

Type "# 4. Targeted repair — re-run only the flagged chunk"
Enter
Sleep 1500ms

Type "rivet repair --config chunked.yaml --export events --execute"
Enter
Sleep 3s

Type "# 5. Committed boundary is untouched (ADR-0009 RR4)"
Enter
Sleep 1200ms

Type "rivet state progression --config chunked.yaml"
Enter
Sleep 3500ms

Type "ls output/ | sort"
Enter
Sleep 3s
