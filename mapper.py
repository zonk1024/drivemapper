#!/usr/bin/env python
import os
import hashlib
import sqlite3
import time
import pprint
from termcolor import colored
path = os.path

class Counter(object):
    def __init__(self):
        self.value = 0
    def incr(self):
        self.value += 1
        return self.value
    def current(self):
        return self.value

cnt = Counter()
debug = True
db_file = 'mapper.db'
commit_size = 8192
err_file = 'mapper.err'
con = sqlite3.connect(db_file)
cur = con.cursor()

SQL = (("metaSQL",  """CREATE TABLE IF NOT EXISTS "main"."meta" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    "action" TEXT NOT NULL,
                    "run" INTEGER NOT NULL,
                    "completed" INTEGER NULL)"""),
       ("dirsSQL",  """CREATE TABLE IF NOT EXISTS "main"."dirs" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    "path" TEXT NOT NULL,
                    "seen" INTEGER NOT NULL,
                    "dirty" INTEGER NOT NULL DEFAULT (0))"""),
       ("filesSQL", """CREATE TABLE IF NOT EXISTS "main"."files" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    "path" TEXT NOT NULL,
                    "md5" TEXT NOT NULL,
                    "stats" TEXT NOT NULL,
                    "seen" INTEGER NOT NULL,
                    "dirty" INTEGER NOT NULL DEFAULT (0))"""),
       ("linksSQL", """CREATE TABLE IF NOT EXISTS "main"."links" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    "path" TEXT NOT NULL,
                    "target_path" TEXT NOT NULL,
                    "seen" INTEGER NOT NULL,
                    "dirty" INTEGER NOT NULL DEFAULT (0))"""),
       ("usersSQL", """CREATE TABLE IF NOT EXISTS "main"."users" (
                    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    "uid" INTEGER NOT NULL,
                    "name" TEXT NOT NULL,
                    "seen" INTEGER NOT NULL,
                    "dirty" INTEGER NOT NULL DEFAULT (0))"""),
       ("groupsSQL", """CREATE TABLE IF NOT EXISTS "main"."groups" (
                     "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                     "gid" INTEGER NOT NULL,
                     "name" TEXT NOT NULL,
                     "seen" INTEGER NOT NULL,
                     "dirty" INTEGER NOT NULL DEFAULT (0))"""),)



def build_tables():
    for table in SQL:
        if debug:
            print "Attempting to create table: {}".format(table[0])
        cur.execute(table[1])
    con.commit()
build_tables()

cur.execute('INSERT INTO "main"."meta" (action, run) VALUES ("testing", ?)', [int(time.time())])
con.commit()
cur.execute('SELECT * FROM "main"."meta"')
res = cur.fetchall()
session = res[-1][0]
pprint.pprint(res)
print 'SESSION: {}'.format(session)
del(res)
time.sleep(1)

def build_users():
    cur.execute("UPDATE users SET dirty=?", [int(time.time())])
    with open('/etc/passwd') as passwd:
        for line in passwd.readlines():
            vals = line.split(':')
            uid = vals[2]
            name = vals[0]
            seen = int(time.time())
            cur.execute('SELECT * FROM "main"."users" WHERE uid=? AND name=?', [uid, unicode(name)])
            res = cur.fetchall()
            if len(res) == 0:
                if debug: print 'INSERT INTO "main"."users" (uid, name, seen) VALUES ({}, "{}", {})'.format(uid, name, seen)
                cur.execute('INSERT INTO "main"."users" (uid, name, seen) VALUES (?, ?, ?)', [uid, unicode(name), seen])
            else:
                if debug: print 'UPDATE "main"."users" SET seen={}, dirty=0'.format(seen)
                cur.execute('UPDATE "main"."users" SET seen=?, dirty=0', [seen])
build_users()

def build_groups():
    cur.execute("UPDATE groups SET dirty=?", [int(time.time())])
    with open('/etc/group') as group:
        for line in group.readlines():
            vals = line.split(':')
            gid = vals[2]
            name = vals[0]
            seen = int(time.time())
            cur.execute('SELECT * FROM "main"."groups" WHERE gid=? AND name=?', [gid, unicode(name)])
            res = cur.fetchall()
            if len(res) == 0:
                if debug: print 'INSERT INTO "main"."groups" (gid, name, seen) VALUES ({}, "{}", {})'.format(gid, unicode(name), seen)
                cur.execute('INSERT INTO "main"."groups" (gid, name, seen) VALUES (?, ?, ?)', [gid, unicode(name), seen])
            else:
                if debug: print 'UPDATE "main"."groups" SET seen={}, dirty=0'.format(seen)
                cur.execute('UPDATE "main"."groups" SET seen=?, dirty=0', [seen])
build_groups()

if debug:
    cur.execute('SELECT * FROM "main"."users" WHERE dirty=0')
    pprint.pprint(cur.fetchall())
    cur.execute('SELECT * FROM "main"."groups" WHERE dirty=0')
    pprint.pprint(cur.fetchall())

def commit_em():
    if debug: print '\n\n\n==============COMMITTING==============\n\n\n'
    con.commit()
    
def safe_unicode(e):
   good = True
   try: e = unicode(e)
   except Exception as e:
       good = False
       ouput = '{}\n{}\n{}\n\n'.format(d, str(e), pprint.pformat(e))
       if debug: print output
       with open(err_file, 'a') as err_f: err_f.write(output)
   if good: return e # slightly prettier than| return (None, e)[good]

def human(b):
    o = 'tgmkb'
    h = {}
    x = 4
    for s in o:
        h[s] = int(b/1024**x)
        b -= 1024**x * h[s]
        x -= 1
    return ' '.join(['{}{}'.format(h[s], s.upper()) for s in 'tgmkb' if sum([h[lulz] for lulz in 'tgmkb'[0:'tgmkb'.find(s) + 1]])>0]) # what a jerk...

def insert_file(f):
    if cnt.incr() % commit_size == 0:
        commit_em()
    f = safe_unicode(f)
    if not f: return
    if debug: print 'count: {:9d}  md5 file size: {:30s}'.format(cnt.current(), colored(human(os.stat(f).st_size), 'red'))

    md5 = file_md5(f)
    stat = ','.join([str(i) for i in os.stat(f)])
    if debug:
        print 'INSERT INTO "main"."files" (path, md5, stats, seen) VALUES ("{}", "{}", "{}", {})'.format(f, md5, stat, int(time.time()))
    cur.execute('INSERT INTO "main"."files" (path, md5, stats, seen) VALUES (?, ?, ?, ?)', [f, unicode(md5), unicode(stat), int(time.time())])

def insert_link(s):
    if cnt.incr() % commit_size == 0:
        commit_em()
    s = safe_unicode(s)
    if not s: return
    target_path = path.realpath(s)
    if debug: print 'INSERT INTO "main"."links" (path, target_path, seen) VALUES ("{}", "{}", {})'.format(s, target_path, int(time.time()))
    cur.execute('INSERT INTO "main"."links" (path, target_path, seen) VALUES (?, ?, ?)', [s, safe_unicode(target_path), int(time.time())])

def insert_dir(d):
    if cnt.incr() % commit_size == 0:
        commit_em()
    d = unicode(d)
    if not d: return
    if debug: print 'INSERT INTO "main"."dirs" (path, seen) VALUES ("{}", {})'.format(d, int(time.time()))
    cur.execute('INSERT INTO "main"."dirs" (path, seen) VALUES (?, ?)', [d, int(time.time())])
    walk_dirs(d)

def get_dirs(p):
    if path.isdir(p): return ['{}/{}'.format(safe_unicode(p), i) for i in os.listdir(p) if path.isdir('{}/{}'.format(p, i)) and not path.islink('{}/{}'.format(p, i))]
    return None

def get_files(p):
    if path.isdir(p): return ['{}/{}'.format(safe_unicode(p), i) for i in os.listdir(p) if path.isfile('{}/{}'.format(p, i)) and not path.islink('{}/{}'.format(p, i))]
    return None

def get_links(p):
    if path.isdir(p): return ['{}/{}'.format(safe_unicode(p), i) for i in os.listdir(p) if path.islink('{}/{}'.format(p, i))]

def file_md5(f):
    if not path.isfile(f) or path.islink(f): return None
    md5 = hashlib.md5()
    with open(f, 'rb') as fo: 
        for chunk in iter(lambda: fo.read(8192), b''): 
            md5.update(chunk)
    return md5.hexdigest()

def walk_dirs(p):
    files = get_files(p)
    links = get_links(p)
    dirs = get_dirs(p)
    for f in files:
        insert_file(f)
    for s in links:
        insert_link(s)
    for d in dirs:
        insert_dir(d)
        walk_dirs(d)
walk_dirs('/media/dumpy')

cur.execute('UPDATE "main"."meta" SET completed=? WHERE id=?', [int(time.time()), int(session)])
con.commit()
cur.close()
con.close()
