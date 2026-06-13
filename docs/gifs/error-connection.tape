# Rivet — `rivet doctor` catches an unreachable source (DB down, wrong host/
# port, or tunnel not up) before a single row is read, with a reachability hint.
# Attach to docs/getting-started.md § "When something is wrong".
#
# Fixture: rivet.yaml (url_env: DATABASE_URL) is written by render.sh
# (fixture_error_connection_setup). This tape overrides DATABASE_URL in the
# hidden prelude to a closed port so the connect deterministically refuses.

Output error-connection.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 540
Set Padding 20
Set TypingSpeed 35ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=error"
Enter
Type "export DATABASE_URL='postgresql://rivet:rivet@localhost:5999/rivet'"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# the database is unreachable — down, wrong host/port, or tunnel not up"
Enter
Sleep 1500ms

Type "rivet doctor -c rivet.yaml"
Enter
Sleep 4s
