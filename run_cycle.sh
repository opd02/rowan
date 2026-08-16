#!/bin/bash
set -euo pipefail

ROWAN_DIR="/home/opd02/rowan"
LOCK_FILE="$ROWAN_DIR/rowan.lock"

exec 9>"$LOCK_FILE"

if ! flock -n 9; then
    echo "Another Rowan cycle is already running. Exiting."
    exit 0
fi

echo "======================================"
echo "Rowan cycle started: $(date)"
echo "======================================"

python3 "$ROWAN_DIR/src/sync_squeue.py"
python3 "$ROWAN_DIR/src/reconcile_sacct.py"
python3 "$ROWAN_DIR/src/process_agent_results.py"
python3 "$ROWAN_DIR/src/dispatch_agents.py"

echo "Rowan cycle finished: $(date)"
echo
