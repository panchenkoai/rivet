# Rivet — CDC access error: a MySQL user missing the REPLICATION grant gets the
# exact requirement (and a pointer to the grants doc), not a cryptic driver error.
#
# Fixture (render.sh, fixture_error_cdc_access_setup): creates a SELECT-only user
# `rivet_noperm` with no REPLICATION privilege.

Output error-cdc-access.gif

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
Type "export NOPERM_URL='mysql://rivet_noperm:x@127.0.0.1:3306/rivet'"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# this user has SELECT but was never granted REPLICATION SLAVE"
Enter
Sleep 1500ms

Type "rivet cdc --source-env NOPERM_URL --table content_items --max-events 1"
Enter
Sleep 4s
