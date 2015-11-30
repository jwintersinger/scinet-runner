#!/bin/bash
set -euo pipefail
export PROT_DIR=~/work/scinet-runner
export INSTANCES_PER_NODE=8

LOGS=~/work/exultant-pistachio/data/logs/runner
mkdir -p $LOGS
$PROT_DIR/run_scinet_tasks.py --concurrent $INSTANCES_PER_NODE \
  >  $LOGS/$SEQNO.stdout \
  2> $LOGS/$SEQNO.stderr
