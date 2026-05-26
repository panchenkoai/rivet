#!/usr/bin/env bash
# Seed pa_audit on every PG and MySQL version container.
# Delegates to dev/matrices/_common/seed_pa_audit_all.sh.
set -eu
exec "$(cd "$(dirname "$0")/../matrices/_common" && pwd)/seed_pa_audit_all.sh" "$@"
