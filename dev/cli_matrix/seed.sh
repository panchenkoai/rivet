#!/usr/bin/env bash
# Seed the `pa_audit` fixture table on both PG and MySQL.
# Delegates to dev/matrices/_common/seed_pa_audit.sh.
set -eu
exec "$(cd "$(dirname "$0")/../matrices/_common" && pwd)/seed_pa_audit.sh" "$@"
