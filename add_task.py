#!/usr/bin/env python2
import argparse
import os
import scinetutil

def create_table(cursor):
  query = '''CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY NOT NULL,
    command TEXT NOT NULL,
    batch_name TEXT NOT NULL,
    priority INT,
    run_dir TEXT NOT NULL,
    retval INT,
    hostname TEXT,
    times_failed INT DEFAULT 0 NOT NULL,
    times_interrupted INT DEFAULT 0 NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
  )'''
  cursor.execute(query)

  query = '''CREATE TABLE IF NOT EXISTS nodes (
    id SERIAL PRIMARY KEY NOT NULL,
    hostname TEXT NOT NULL,
    process_id INT NOT NULL,
    job_id TEXT,
    physical_cpus INT NOT NULL,
    logical_cpus INT NOT NULL,
    mem_free INT8,
    mem_used INT8,
    load_avg_1min REAL,
    load_avg_5min REAL,
    load_avg_15min REAL,
    cpu_usage REAL,
    num_tasks INT,
    started_at TIMESTAMPTZ NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL
  )'''
  cursor.execute(query)

def add_task(cursor, command, batch_name, run_dir, priority):
  run_dir = os.path.realpath(run_dir)
  query = '''
    INSERT INTO tasks (command, batch_name, run_dir, priority)
    VALUES (%s, %s, %s, %s)
  '''
  cursor.execute(query, (command, batch_name, run_dir, priority))

def main():
  parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
  )
  parser.add_argument('-p', '--priority', dest='priority', type=int,
    help='Priority for dataset (lower = higher priority)')
  parser.add_argument('command',
    help='Shell command')
  parser.add_argument('batch_name',
    help='Batch name')
  parser.add_argument('run_dir',
    help='Working directory for command')
  args = parser.parse_args()

  conn = scinetutil.db_connect()
  cursor = conn.cursor()
  create_table(cursor)
  add_task(cursor, args.command, args.batch_name, args.run_dir, args.priority)
  conn.commit()
  conn.close()

if __name__ == '__main__':
  main()
