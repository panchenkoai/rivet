# Rivet — a one-character config typo (`export:` instead of `exports:`) is
# caught at parse time with a "Did you mean" suggestion, naming the line.
# Attach to docs/getting-started.md § "When something is wrong".
#
# Fixture: rivet.yaml is written by render.sh (fixture_error_config_typo_setup).
# Parsing fails before any connection, so no live database is needed.

Output error-config-typo.gif

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
Sleep 3500ms
