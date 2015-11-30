import math
import psycopg2
import random
import sys
import time
import traceback
from config import CONFIG
from datetime import datetime
from contextlib import contextmanager

def db_connect():
  conn = psycopg2.connect('dbname=%s user=%s password=%s host=%s port=%s' % (
    CONFIG['db_name'],
    CONFIG['db_username'],
    CONFIG['db_password'],
    CONFIG['db_host'],
    CONFIG['db_port'],
  ))
  conn.autocommit = True
  return conn

class DbConnManager(object):
  def __init__(self):
    self.reconnect()

  def reconnect(self):
    self._conn = db_connect()

  def cursor(self):
    cursor = self._conn.cursor()
    return cursor

  def close(self):
    return self._conn.close()

def calc_delay(old_delay):
  delay_log = math.floor(math.log(old_delay, 2))
  new_delay = random.randint(2**(delay_log + 1), 2**(delay_log + 2))
  return min(32, new_delay)

@contextmanager
def transaction(conn_manager, must_exit, lock_tasks):
  must_reconnect = False
  delay = 2
  times_failed = 0
  max_times_to_fail = 10

  while True:
    if must_exit.is_set():
      logmsg('must_exit is set.')
      sys.exit(3)
    if times_failed >= max_times_to_fail:
      logmsg('Failed %s times. Exiting.' % times_failed)
      sys.exit(4)

    try:
      if must_reconnect:
        try:
          conn_manager.close()
        except psycopg2.OperationalError as opperr_inner:
          pass
        conn_manager.reconnect()
        must_reconnect = False
      cursor = conn_manager.cursor()
      cursor.execute('BEGIN')
      if lock_tasks:
        cursor.execute('SET lock_timeout TO 5000')
        cursor.execute('LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE')
    except psycopg2.OperationalError as opperr:
      times_failed += 1
      delay = calc_delay(delay)
      #traceback.print_exc()
      logmsg('OperationalError occurred. Trying again in %s s ...' % delay, sys.stderr)
      time.sleep(delay)
      must_reconnect = True
    else:
      yield cursor
      break
    finally:
      try:
        cursor.execute('COMMIT')
      except psycopg2.OperationalError as opperr_inner:
        pass

def logmsg(msg, fd=sys.stdout):
  print >> fd, '[%s] %s' % (datetime.now(), msg)
