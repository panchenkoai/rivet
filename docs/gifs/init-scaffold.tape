# Rivet — rivet init scaffolds a YAML config from a live table.
# Attach to docs/getting-started.md § "Scaffold a config with rivet init".

Output init-scaffold.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 560
Set Padding 20
Set TypingSpeed 35ms
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

Type "rivet init --source-env DATABASE_URL --table orders -o orders.yaml"
Enter
Sleep 2500ms

Type "cat orders.yaml"
Enter
Sleep 3s
