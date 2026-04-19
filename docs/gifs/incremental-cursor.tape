# Rivet — incremental mode: cursor is saved on first run; second run
# exports zero rows and skips the file when `skip_empty: true`.

Output incremental-cursor.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 720
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

Type "# First run — no cursor yet, all 10,000 rows exported"
Enter
Sleep 1500ms

Type "rivet run --config incremental.yaml --validate"
Enter
Sleep 3500ms

Type "rivet state show --config incremental.yaml"
Enter
Sleep 2500ms

Type "# Second run — cursor is now 10000; nothing new. skip_empty suppresses the file."
Enter
Sleep 2s

Type "rivet run --config incremental.yaml --validate"
Enter
Sleep 3s

Type "rivet state show --config incremental.yaml"
Enter
Sleep 2500ms
