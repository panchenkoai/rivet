# Rivet — `rivet check` catches a query against a table that doesn't exist,
# before any data is exported. SQLSTATE class 42 is permanent + author-fixable,
# so check fails loudly (exit 1) instead of passing through to run time.
# Attach to docs/getting-started.md § "When something is wrong".
#
# Fixture: rivet.yaml is written by render.sh (fixture_error_missing_table_setup);
# DATABASE_URL points at the live Postgres so the connect succeeds and only the
# schema probe fails.

Output error-missing-table.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 460
Set Padding 20
Set TypingSpeed 35ms
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

Type "cat rivet.yaml"
Enter
Sleep 2200ms

Type "rivet check -c rivet.yaml"
Enter
Sleep 4s
