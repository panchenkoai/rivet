# Rivet — basic workflow: init -> doctor -> check -> run -> state.
# Rendered by docs/gifs/render.sh (which sets CWD to /tmp/rivet-gif-basic
# and exports RIVET_BIN_DIR + DATABASE_URL).

Output basic.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 720
Set Padding 20
Set TypingSpeed 35ms
Set PlaybackSpeed 1
Set Theme "Dracula"

# --- Invisible shell priming ------------------------------------------------
Hide
Type `export PATH=$RIVET_BIN_DIR:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=error"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 500ms
Show

# --- Scenario ---------------------------------------------------------------
Type "rivet --version"
Enter
Sleep 1200ms

Type "rivet init --source-env DATABASE_URL --table orders -o orders.yaml"
Enter
Sleep 2s

Type "cat orders.yaml"
Enter
Sleep 2500ms

Type "rivet doctor -c orders.yaml"
Enter
Sleep 2s

Type "rivet check -c orders.yaml"
Enter
Sleep 2500ms

Type "rivet run -c orders.yaml --validate --reconcile"
Enter
Sleep 12s

Type "rivet state show -c orders.yaml"
Enter
Sleep 2s

Type "ls output/"
Enter
Sleep 2s
