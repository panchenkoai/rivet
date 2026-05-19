# Rivet — pooler/proxy detection warnings emitted by rivet run at connect time.
#
# Demonstrates the MySQL / ProxySQL signal (PROXYSQL INTERNAL SESSION
# interception — works regardless of backend pool size). Detection is
# triggered from MysqlSource::connect, which only the `rivet run` path
# exercises; `rivet check` uses connect_pool() which bypasses detection.
#
# The Postgres analog (pg_backend_pid() drift via two consecutive queries)
# uses the same wiring — it just happens that the docker-compose pgBouncer
# is configured with pool_size=1 for the live pool-safety tests, which is
# the documented false-negative case for the PID-drift heuristic. See
# docs/architecture.md § "Connection pooler / proxy detection" for the
# full signal precedence on both drivers.
#
# Fixture: render.sh writes two YAML configs in the workdir and assumes
# the `pool` docker-compose profile is already up
# (`docker compose --profile pool up -d proxysql`). Skipped from the
# default render.sh invocation — opt in via:
#   ./docs/gifs/render.sh pool-detect

Output pool-detect.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 640
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:$(dirname $PSQL_BIN):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type "export RUST_LOG=warn"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# 1. Direct MySQL (3306) — no proxy detected, no warning at connect"
Enter
Sleep 1500ms

Type `rivet run --config direct-mysql.yaml 2>&1 | grep -E 'MySQL proxy|── probe|run_id|status|rows|duration'`
Enter
Sleep 4s

Type "# 2. Same query via ProxySQL (6033) — multiplexer warning fires at connect"
Enter
Sleep 1500ms

Type `rivet run --config proxysql.yaml 2>&1 | grep -E 'MySQL proxy|── probe|run_id|status|rows|duration'`
Enter
Sleep 5s
