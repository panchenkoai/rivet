#!/usr/bin/env bash
exec "$(cd "$(dirname "$0")/../matrices/_common/lib" && pwd)/extract_summary.sh" "$@"
