#!/usr/bin/env python2
import argparse
import os
import psutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import scinetutil
from config import CONFIG
import cProfile
import os

logmsg = scinetutil.logmsg

class ScinetRunner(object):
  def __init__(self, must_exit):
    self._must_exit = must_exit
    self._processes = {}
    logmsg('Connecting ...')
    self._conn = scinetutil.DbConnManager()
    logmsg('Connected.')

  def _launch_task(self, task_id, command, rundir):
    if not os.path.exists(rundir):
      os.makedirs(rundir)
    if not os.path.exists(CONFIG['std_streams_path']):
      os.makedirs(CONFIG['std_streams_path'])

    stdout_path = os.path.join(CONFIG['std_streams_path'], '%s.stdout' % task_id)
    stderr_path = os.path.join(CONFIG['std_streams_path'], '%s.stderr' % task_id)

    with open(stdout_path, 'w') as stdout:
      with open(stderr_path, 'w') as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, cwd=rundir, shell=True, executable='/bin/bash')
    return process

  def _mark_started(self, task_id, trx):
    logmsg('starting task=%s' % task_id)

    query = '''
      UPDATE tasks SET
        node_id = %s,
        started_at = NOW()
      WHERE id = %s
    '''

    return trx.execute(query, (self._node_id, task_id))

  def _mark_interrupted(self, task_id, trx):
    logmsg('stopped task=%s' % task_id)

    query = '''
      UPDATE tasks SET
        node_id = NULL,
        times_interrupted = times_interrupted + 1
      WHERE id = %s
    '''

    return trx.execute(query, (task_id,))

  def _mark_finished(self, task_id, retval, trx):
    logmsg('finished task=%s retval=%s' % (task_id, retval))

    query = '''
      UPDATE tasks SET
        retval = %s,
        node_id = NULL,
    '''
    if retval != 0:
      query += 'times_failed = times_failed + 1,'
    query += '''
        finished_at = NOW()
      WHERE id = %s
    '''

    return trx.execute(query, (retval, task_id))

  def _update_finished_tasks(self):
    finished = []
    for task_id, process in self._processes.items():
      retval = process.poll()
      if retval is None: # Process still running
        continue
      else:
        del self._processes[task_id]
        finished.append((task_id, retval))

    if len(finished) > 0:
      with scinetutil.transaction(self._conn, self._must_exit, True) as trx:
        for task_id, retval in finished:
          self._mark_finished(task_id, retval, trx)

  def _launch_new_tasks(self, num_concurrent_tasks):
    num_to_launch = num_concurrent_tasks - len(self._processes)
    if num_to_launch <= 0:
      return

    with scinetutil.transaction(self._conn, self._must_exit, True) as trx:
      # By default, NULLs get sorted *after* non-null values, which is what we
      # want when sorting on priority -- *any* priority value is treated as more
      # important than no priority value.
      query = '''
        SELECT id, command, run_dir
        FROM tasks
        WHERE
          is_active = TRUE AND
          started_at IS NULL AND
          times_failed = 0 AND
          times_interrupted = 0
        ORDER BY priority, batch_name
        LIMIT %s
      '''
      trx.execute(query, (num_to_launch,))
      results = trx.fetchall()
      if len(results) == 0:
        return

      for task_id, command, run_dir in results:
        process = self._launch_task(task_id, command, run_dir)
        self._processes[task_id] = process
        self._mark_started(task_id, trx)

  def _insert_node_status(self):
    if 'PBS_JOBID' in os.environ:
      job_id = os.environ['PBS_JOBID']
    else:
      job_id = None
    with scinetutil.transaction(self._conn, self._must_exit, False) as trx:
      query = '''INSERT INTO nodes (
        hostname,
        process_id,
        job_id,
        physical_cpus,
        logical_cpus,
        created_at,
        last_updated
      ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
      RETURNING id
      '''
      trx.execute(query, (
        socket.gethostname(),
        os.getpid(),
        job_id,
        psutil.cpu_count(logical=False),
        psutil.cpu_count(logical=True),
      ))

      node_id = trx.fetchone()[0]
    return node_id

  def _update_node_status(self):
    query = '''UPDATE nodes SET
      load_avg_1min = %s,
      load_avg_5min = %s,
      load_avg_15min = %s,
      cpu_usage = %s,
      mem_free = %s,
      mem_used = %s,
      num_tasks = %s,
      last_updated = NOW()
      WHERE id = %s
    '''

    load_avg = os.getloadavg()
    mem_usage = psutil.virtual_memory()
    with scinetutil.transaction(self._conn, self._must_exit, False) as trx:
      trx.execute(query, (
        load_avg[0],
        load_avg[1],
        load_avg[2],
        psutil.cpu_percent(interval=0.5),
        mem_usage.available,
        mem_usage.total - mem_usage.available,
        len(self._processes),
        self._node_id
      ))

  def _delete_node_status(self, trx):
    query = 'DELETE FROM nodes WHERE id = %s'
    trx.execute(query, (self._node_id,))

  def start_tasks(self, num_concurrent_tasks):
    iteration = 0
    self._node_id = self._insert_node_status()
    logmsg('Inserted node status')

    while True:
      self._update_finished_tasks()
      self._launch_new_tasks(num_concurrent_tasks)
      #logmsg('tick')

      # Periodically update status.
      # This works out to every 600 s.
      if iteration % 120 == 0:
        #logmsg('Updating node status')
        self._update_node_status()

      if len(self._processes) == 0:
        logmsg('All tasks finished.')
        break

      # Sleep for five seconds.
      self._must_exit.wait(5.0)
      if self._must_exit.is_set():
        self.terminate_run()
        return
      iteration += 1

    self._conn.close()
    sys.exit()

  def terminate_run(self):
    terminated_ids = []
    for task_id, process in self._processes.items():
      logmsg('Terminating task=%s' % task_id)
      process.terminate()
      terminated_ids.append(task_id)

    with scinetutil.transaction(self._conn, self._must_exit, True) as trx:
      for task_id in terminated_ids:
        self._mark_interrupted(task_id, trx)
    with scinetutil.transaction(self._conn, self._must_exit, False) as trx:
      self._delete_node_status(trx)
    self._conn.close()

    logmsg('Sleeping before exit ...')
    time.sleep(3)
    logmsg('Waking before exit ...')

def run(must_exit):
  parser = argparse.ArgumentParser(
    description='Run tasks on SciNet in parallel',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )
  parser.add_argument('--concurrent', '-c', type=int, default=8,
    help='Number of concurrent tasks to run')
  args = parser.parse_args()

  _run = lambda: ScinetRunner(must_exit).start_tasks(args.concurrent)
  if 'PROFILE' in os.environ and os.environ['PROFILE'] == '1':
    cProfile.runctx('_run()', globals=globals(), locals=locals(), filename='lol.prof')
  else:
    _run()

def main():
  must_exit = threading.Event()

  def sigterm_handler(_signo, _stack_frame):
    logmsg('Exit signal received.')
    must_exit.set()
  signal.signal(signal.SIGTERM, sigterm_handler)
  signal.signal(signal.SIGINT, sigterm_handler)

  run_thread = threading.Thread(target=run, args=(must_exit,))
  run_thread.start()
  # Can't just call signal.pause() instead of using this loop, as it will cause
  # program to hang waiting for signal if exception occurs in run_thread.
  while True:
    if not run_thread.is_alive():
      logmsg('Run thread is no longer alive')
      break
    run_thread.join(10)

  if must_exit.set():
    # Exit with non-zero to indicate run didn't finish.
    logmsg('must_exit is set.')
    sys.exit(3)
  else:
    logmsg('Finished. Exiting.')
    sys.exit()

if __name__ == '__main__':
  main()
