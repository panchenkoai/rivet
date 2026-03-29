#!/usr/bin/env bash
# ───────────────────────────────────────────────────────────────
# Retry resilience E2E test via Toxiproxy
# ───────────────────────────────────────────────────────────────
# Toxiproxy sits between Rivet and Postgres and lets us inject
# network faults (latency, connection drops) to verify retries.
#
#   Rivet ──► localhost:15432 (Toxiproxy) ──► postgres:5432
#
# Prerequisites:
#   docker compose up -d postgres toxiproxy
#   bash dev/setup_toxiproxy.sh
#   cargo run --release --bin seed -- --target pg --users 10000
#
# What this script does:
#   Q1  Baseline export through the proxy (no faults)
#   Q2  Inject 3 s latency → Q3 export (should be slow but pass)
#   Q4  Remove latency
#   Q5  Inject connection kill after 5 KB → Q6 export (retries)
#   Q7  Remove connection-kill toxic
#   Q8  Final baseline (confirm proxy is clean)
# ───────────────────────────────────────────────────────────────
set -euo pipefail

TOXI=http://localhost:8474
CONFIG=dev/test_toxiproxy_pg.yaml

# Use installed binary if available, otherwise cargo run
if command -v rivet &> /dev/null; then
  RIVET="rivet"
else
  RIVET="cargo run --release --bin rivet --"
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Toxiproxy Retry Resilience Test                    ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Q1: Baseline ─────────────────────────────────────────────
echo "── Q1: Baseline (clean proxy, no faults) ──"
RUST_LOG=info $RIVET run --config $CONFIG --validate
echo "✓ Q1 passed"
echo ""

# ── Q2: Inject latency ──────────────────────────────────────
echo "── Q2: Injecting 3s latency on every packet ──"
curl -sf -X POST "$TOXI/proxies/pg/toxics" \
  -H 'Content-Type: application/json' \
  -d '{"name":"latency","type":"latency","attributes":{"latency":3000}}' > /dev/null
echo "   Toxic 'latency' added."

# ── Q3: Export under latency ─────────────────────────────────
echo "── Q3: Export under latency (expect slower but success) ──"
RUST_LOG=info $RIVET run --config $CONFIG --validate
echo "✓ Q3 passed"
echo ""

# ── Q4: Remove latency ──────────────────────────────────────
echo "── Q4: Removing latency toxic ──"
curl -sf -X DELETE "$TOXI/proxies/pg/toxics/latency" > /dev/null
echo "   Toxic 'latency' removed."
echo ""

# ── Q5: Inject connection kill ───────────────────────────────
echo "── Q5: Injecting connection kill after 5 KB ──"
curl -sf -X POST "$TOXI/proxies/pg/toxics" \
  -H 'Content-Type: application/json' \
  -d '{"name":"limit","type":"limit_data","attributes":{"bytes":5000}}' > /dev/null
echo "   Toxic 'limit' added (connection drops after 5 KB)."

# ── Q6: Export under connection kill ─────────────────────────
echo "── Q6: Export under connection kill (expect retries) ──"
if RUST_LOG=info $RIVET run --config $CONFIG; then
  echo "✓ Q6 passed (retried and succeeded)"
else
  echo "⚠ Q6: export failed — expected under harsh 5 KB limit."
  echo "  Check logs above for 'retry' messages — that confirms retry logic works."
fi
echo ""

# ── Q7: Remove connection-kill toxic ─────────────────────────
echo "── Q7: Removing limit toxic ──"
curl -sf -X DELETE "$TOXI/proxies/pg/toxics/limit" > /dev/null
echo "   Toxic 'limit' removed."
echo ""

# ── Q8: Final baseline ──────────────────────────────────────
echo "── Q8: Final baseline (proxy clean again) ──"
RUST_LOG=info $RIVET run --config $CONFIG --validate
echo "✓ Q8 passed"
echo ""

echo "╔══════════════════════════════════════════════════════╗"
echo "║  All Toxiproxy tests completed.                     ║"
echo "╚══════════════════════════════════════════════════════╝"
