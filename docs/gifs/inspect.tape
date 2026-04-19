# Rivet — post-run inspection commands.
# Attach to docs/getting-started.md § "Inspect results".
#
# Fixture: render.sh seeds an incremental export YAML, runs it once against
# rivet_gif.events so state show / metrics / state files / state progression
# all have data to display.

Output inspect.gif

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

Type "rivet state show --config orders_incremental.yaml"
Enter
Sleep 2500ms

Type "rivet metrics --config orders_incremental.yaml --last 5"
Enter
Sleep 3s

Type "rivet state files --config orders_incremental.yaml --last 5"
Enter
Sleep 3s

Type "rivet state progression --config orders_incremental.yaml"
Enter
Sleep 3s
