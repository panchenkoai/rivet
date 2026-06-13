# Rivet — incremental + COALESCE(updated_at, created_at) fallback cursor.
# Shows what plain `cursor_column: updated_at` would miss: rows where
# updated_at is NULL but created_at is set. ADR-0007 CC1–CC6.

Output coalesce-cursor.gif

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

Type "# Source: 200 orders, ~35% have NULL updated_at (only created_at is set)"
Enter
Sleep 1500ms

Type `psql "$DATABASE_URL" -c "SELECT count(*) AS total, count(*) FILTER (WHERE updated_at IS NULL) AS null_updated FROM rivet_gif.orders;"`
Enter
Sleep 2500ms

Type "# Config uses incremental_cursor_mode: coalesce → cursor is COALESCE(updated_at, created_at)"
Enter
Sleep 1500ms

Type "cat orders.yaml"
Enter
Sleep 3s

Type "# First run — all 200 rows exported, including NULL-updated_at ones"
Enter
Sleep 1500ms

Type "rivet run -c orders.yaml --validate"
Enter
Sleep 3500ms

Type "rivet state show -c orders.yaml"
Enter
Sleep 2500ms

Type "# Insert 30 NEW orders with updated_at = NULL (typical late-arriving inserts)"
Enter
Sleep 1500ms

Type `psql "$DATABASE_URL" -c "INSERT INTO rivet_gif.orders (id, product, quantity, price, updated_at, created_at) SELECT 200 + g, 'late-' || g, g, 9.99, NULL, NOW() FROM generate_series(1, 30) g;"`
Enter
Sleep 2s

Type "# Second run — COALESCE picks them up via created_at. A plain cursor_column: updated_at would skip them."
Enter
Sleep 2s

Type "rivet run -c orders.yaml --validate"
Enter
Sleep 3500ms

Type "rivet state show -c orders.yaml"
Enter
Sleep 2500ms
