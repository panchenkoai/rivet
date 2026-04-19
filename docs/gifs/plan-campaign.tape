# Rivet — multi-export `rivet plan` with source_group collision.
# The Campaign block surfaces waves + `source_group_warnings[]` so operators
# can stagger heavy exports sharing one replica.

Output plan-campaign.gif

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

Type "# Three exports, two share `source_group: replica_main` and are both heavy"
Enter
Sleep 1500ms

Type "rivet plan --config campaign.yaml"
Enter
Sleep 6s
