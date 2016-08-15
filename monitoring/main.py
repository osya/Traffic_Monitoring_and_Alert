#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb


def connect():
    conn = MySQLdb.connect(host="209.126.102.168",
                         port=3306,
                         user="root",
                         passwd="test123#",
                         db="test")
    return conn.cursor(), conn

if __name__ == '__main__':
    cur, db = connect()

    cur.execute("SELECT * FROM demo_cdr;")

    data = cur.fetchall()

    print "Data : %s " % str(data)

    db.close()


