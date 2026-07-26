#!/usr/bin/env bash
# Regenerate golden_catalog.db from the versioned SQL source. The .sql is the
# diffable source of truth; the .db is the ready-to-query artifact both are checked
# in so a consumer can query without a build step, but the .db must be REBUILT (not
# hand-edited) whenever golden_catalog.sql changes.
set -euo pipefail
cd "$(dirname "$0")"
rm -f golden_catalog.db
sqlite3 golden_catalog.db < golden_catalog.sql
n=$(sqlite3 golden_catalog.db "SELECT count(*) FROM golden_seed")
echo "built golden_catalog.db — $n seeds ($(sqlite3 golden_catalog.db "SELECT count(*) FROM golden_seed WHERE category='normal'") normal, $(sqlite3 golden_catalog.db "SELECT count(*) FROM golden_seed WHERE category='garbage'") garbage)"
