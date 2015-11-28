import psycopg2
import sys
import time
import traceback
from config import CONFIG
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
    cursor.execute('SET lock_timeout TO 5000')
    return cursor

  def close(self):
    return self._conn.close()

@contextmanager
def transaction(conn_manager, lock_tasks):
  must_reconnect = False
  while True:
    try:
      if must_reconnect:
        conn_manager.reconnect()
        must_reconnect = False
      cursor = conn_manager.cursor()
      cursor.execute('BEGIN')
      if lock_tasks:
        cursor.execute('LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE')
    except psycopg2.OperationalError as opperr:
      delay = 3
      traceback.print_exc()
      print >> sys.stderr, 'OperationalError occurred. Trying again in %s s ...' % delay
      time.sleep(delay)
      must_reconnect = True
    else:
      yield cursor
      cursor.execute('COMMIT')
      return
