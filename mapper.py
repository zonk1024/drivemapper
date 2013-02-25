#!/usr/bin/env python
import os
import hashlib
import sqlite3
import time
import pprint
path = os.path

debug = True

SQL = (
    ("metaSQL", """CREATE TABLE IF NOT EXISTS "main"."meta" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "action" TEXT NOT NULL,
        "run" INTEGER NOT NULL,
        "completed" INTEGER NULL
        )"""),
    ("dirsSQL", """CREATE TABLE IF NOT EXISTS "main"."dirs" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "path" TEXT NOT NULL,
        "seen" INTEGER NOT NULL,
        "dirty" INTEGER NOT NULL DEFAULT (0)
        )"""),
    ("filesSQL", """CREATE TABLE IF NOT EXISTS "main"."files" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "path" TEXT NOT NULL,
        "md5" TEXT NOT NULL,
        "stats" TEXT NOT NULL,
        "seen" INTEGER NOT NULL,
        "dirty" INTEGER NOT NULL DEFAULT (0)
        )"""),
    ("linksSQL", """CREATE TABLE IF NOT EXISTS "main"."links" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "path" TEXT NOT NULL,
        "target_path" TEXT NOT NULL,
        "seen" INTEGER NOT NULL,
        "dirty" INTEGER NOT NULL DEFAULT (0)
        )"""),
    ("usersSQL", """CREATE TABLE IF NOT EXISTS "main"."users" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "uid" INTEGER NOT NULL,
        "name" TEXT NOT NULL,
        "seen" INTEGER NOT NULL,
        "dirty" INTEGER NOT NULL DEFAULT (0)
        )"""),
    ("groupsSQL", """CREATE TABLE IF NOT EXISTS "main"."groups" (
        "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        "gid" INTEGER NOT NULL,
        "name" TEXT NOT NULL,
        "seen" INTEGER NOT NULL,
        "dirty" INTEGER NOT NULL DEFAULT (0)
        )"""),
)

con = sqlite3.connect('mapper.sqlite3')
cur = con.cursor()

def buildTables():
    for table in SQL:
        if debug:
            print "Attempting to create table: {}".format(table[0])
        cur.execute(table[1])
buildTables()

cur.execute('INSERT INTO "main"."meta" (action, run) VALUES ("testing", ?)', [int(time.time())])
con.commit()
cur.execute('SELECT * FROM "main"."meta"')
res = cur.fetchall()
session = res[-1][0]
pprint.pprint(res)

def buildUsers():
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
                if debug:
                    print 'INSERT INTO "main"."users" (uid, name, seen) VALUES ({}, "{}", {})'.format(uid, name, seen)
                cur.execute('INSERT INTO "main"."users" (uid, name, seen) VALUES (?, ?, ?)', [uid, unicode(name), seen])
            else:
                if debug:
                    print 'UPDATE "main"."users" SET seen={}, dirty=0'.format(seen)
                cur.execute('UPDATE "main"."users" SET seen=?, dirty=0', [seen])
buildUsers()

if debug:
    cur.execute('SELECT * FROM "main"."users" WHERE dirty=0')
    pprint.pprint(cur.fetchall())


def buildGroups():
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
                if debug:
                    print 'INSERT INTO "main"."groups" (gid, name, seen) VALUES ({}, "{}", {})'.format(gid, unicode(name), seen)
                cur.execute('INSERT INTO "main"."groups" (gid, name, seen) VALUES (?, ?, ?)', [gid, unicode(name), seen])
            else:
                if debug:
                    print 'UPDATE "main"."groups" SET seen={}, dirty=0'.format(seen)
                cur.execute('UPDATE "main"."groups" SET seen=?, dirty=0', [seen])
buildGroups()

def saveDir(p):
    cur.execute('SELECT * FROM "main"."dirs" WHERE path=?', [p])
    if len(cur.fetchall) != 0:
        cur.execute('INSERT INTO "main"."dirs" (path, seen) VALUES (?, ?)', [p, int(time.time())])

if debug:
    cur.execute('SELECT * FROM "main"."groups" WHERE dirty=0')
    pprint.pprint(cur.fetchall())


def getDirs(p):
    if path.isdir(p):
        return ['{}/{}'.format(p, i) for i in os.listdir(p) if path.isdir('{}/{}'.format(p, i)) and not path.islink('{}/{}'.format(p, i))]
    return None

def getFiles(p):
    if path.isdir(p):
        return ['{}/{}'.format(p, i) for i in os.listdir(p) if path.isfile('{}/{}'.format(p, i)) and not path.islink('{}/{}'.format(p, i))]
    return None

def getLinks(p):
    if path.isdir(p):
        return ['{}/{}'.format(p, i) for i in os.listdir(p) if path.islink('{}/{}'.format(p, i))]

def fileMD5(f):
    if not path.isfile(f) or path.islink(f):
        return None
    md5 = hashlib.md5()
    with open(f, 'rb') as fo: 
        for chunk in iter(lambda: fo.read(8192), b''): 
            md5.update(chunk)
    return md5.hexdigest()

cnt = 0
def walkDirs(p):
    global cnt
    cnt += 1
    if cnt % 1000 == 0:
        con.commit()
    print 'INSERT INTO "main"."dirs" (path, seen) VALUES ("{}", {})'.format(p, int(time.time()))
    cur.execute('INSERT INTO "main"."dirs" (path, seen) VALUES (?, ?)', [unicode(p), int(time.time())])
    dirs = getDirs(p)
    for i in dirs:
        try:
            walkDirs(unicode(i))
        except Exception as e:
            print e
            with open('mapper.err', 'a') as errFile:
                errFile.write('{}\n{}\n\n'.format(i, e))
walkDirs(os.getcwd())

cur.execute('UPDATE "main"."meta" SET completed=?', [int(time.time())])
con.commit()
cur.close()
con.close()
