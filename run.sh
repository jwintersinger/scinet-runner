#!/bin/bash
set -euo pipefail

function set_vars {
  export PROT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
}

function launch_runners {
  cd ~/jobs
  for i in $(seq 100); do
    # Walltime format: hh:mm:ss
    qsub \
      -l "nodes=1:ppn=8,walltime=46:00:00" \
      -v "SEQNO=$i" \
      $PROT_DIR/_run_instance.sh
  done
}

function main {
  set_vars
  launch_runners
}

main
