#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import psycopg2
import psycopg2.extras
import smtplib
import logging


def connect_to_memsql(host, port, user, password, db):
    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
    except:
        raise 'Unable to connect to the MemSQL'
    return conn


def connect_to_postgresql(host, port, database, user, password):
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    except:
        raise 'I am unable to connect to the PostgreSQL'
    return conn, cursor


def read_monitoring_rules():
    conn, cur = connect_to_postgresql('localhost', 5432, 'postgres')
    # TODO: вкрячить def alert_rule(cursor):


if __name__ == '__main__':
    logger = logging.getLogger('Monitoring Rule')

    # read_monitoring_rules()

    # memsql = connect_to_memsql(host="209.126.102.168", port=3306, user="root", passwd="test123#", db="test")
    # cur = memsql.cursor()
    #
    # cur.execute("SELECT * FROM demo_cdr;")
    #
    # data = cur.fetchall()
    #
    # print "Data : %s " % str(data)
    #
    # memsql.close()
