# Rivet — change data capture: scaffold a cdc config, capture INSERT/UPDATE/DELETE
# from the MySQL binlog into typed Parquet, then read it back — every change is a
# typed row (__op + __pos + the columns).
#
# Fixture (render.sh, fixture_cdc_setup): seeds a checkpoint at the current binlog
# position and a small backlog of changes after it, so the run drains a
# deterministic set and exits.

Output cdc.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 720
Set Padding 20
Set TypingSpeed 35ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=error"
Enter
Type "export MYSQL_URL='mysql://rivet:rivet@127.0.0.1:3306/rivet'"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 500ms
Show

Type "# CDC reads the transaction log — no long query, no lock."
Enter
Sleep 1200ms

Type "rivet init --source-env MYSQL_URL --table content_items --mode cdc -o cdc.yaml"
Enter
Sleep 1800ms

Type "sed -n '/  - name:/,/server_id/p' cdc.yaml"
Enter
Sleep 3s

Type "# capture changes since the checkpoint into typed Parquet, then exit"
Enter
Sleep 1000ms

Type "rivet cdc --source-env MYSQL_URL --table content_items --checkpoint cdc.ckpt --until-current --output ./cdc-out"
Enter
Sleep 3s

Type "# read it back — one typed row per change: __op + the columns"
Enter
Sleep 1000ms

Type `duckdb -c "SELECT __op, id, status, view_count, updated_at FROM read_parquet('cdc-out/*.parquet')"`
Enter
Sleep 4s
