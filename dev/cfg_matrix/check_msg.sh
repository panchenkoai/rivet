#!/usr/bin/env bash
# Enforce per-(scenario, probe, stream) substring contracts from expected_msg.txt.
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT/../matrices/_common/lib/check_msg.sh" \
  --contract "$ROOT/expected_msg.txt" \
  --logs "$ROOT/logs" \
  --layout probe
