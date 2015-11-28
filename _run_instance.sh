#!/bin/bash
set -euo pipefail
export PROT_DIR=~/work/scinet-runner
export INSTANCES_PER_NODE=8

$PROT_DIR/run_scinet_tasks.py --concurrent $INSTANCES_PER_NODE
