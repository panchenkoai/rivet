#!/usr/bin/env bash
# Seed pa_soak on primary PG. Delegates to dev/matrices/_common/seed_pa_soak.sh.
set -eu
exec "$(cd "$(dirname "$0")/../matrices/_common" && pwd)/seed_pa_soak.sh" "$@"
