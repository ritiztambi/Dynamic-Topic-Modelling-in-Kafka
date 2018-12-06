import sqlite3
from sqlite3 import Error
import datetime
import time
import os
import csv

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
    return None



def create_table_topics(conn):
    create_table_sql = """ CREATE TABLE IF NOT EXISTS Topics (
                                        ID integer PRIMARY KEY AUTOINCREMENT,
                                        Name text NOT NULL,
                                        Parent_ID integer,
                                        Timestamp text
                                    ); """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        


def create_table_actions(conn):
    create_table_sql = """ CREATE TABLE IF NOT EXISTS Actions (
                                        Name text NOT NULL,
                                        Action text,
                                        Timestamp text
                                    ); """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)



def delete_table_topics(conn):
    delete_table_sql = """ DROP TABLE IF EXISTS Topics"""
    try:
        c = conn.cursor()
        c.execute(delete_table_sql)
    except Error as e:
        print(e)           



def delete_table_actions(conn):
    delete_table_sql = """ DROP TABLE IF EXISTS Actions"""
    try:
        c = conn.cursor()
        c.execute(delete_table_sql)
    except Error as e:
        print(e)  
         


def create_topic(conn, topic):
    sql = ''' INSERT INTO Topics(ID,Name,Parent_ID,Timestamp)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, topic)
    return cur.lastrowid



def create_action(conn, action):
    sql = ''' INSERT INTO Actions(Name,Action,Timestamp)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, action)
    return cur.lastrowid



def delete_topic(conn, id):
    cur = conn.cursor()
    sql = ''' DELETE FROM Topics Where ID = ?'''
    cur.execute(sql, (id,))
    sql = ''' DELETE FROM Topics Where Parent_ID = ?'''
    cur.execute(sql, (id,))
    return cur.lastrowid



def merge_topics(conn, name):
    sql = ''' select ID from Topics where name = ?'''
    cur = conn.cursor()
    cur.execute(sql, (name,))
    parent_id = cur.fetchone()[0]
    
    sql = ''' DELETE FROM Topics Where Parent_ID = ?'''
    cur = conn.cursor()
    cur.execute(sql, (parent_id,))
    os.system('/usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}.*'.format(name))
    return cur.lastrowid


def split_topics(conn, name):
    sql = ''' Select count(*) from Topics where Parent_ID = (select ID from topics where name = ?)'''
    cur = conn.cursor()
    cur.execute(sql, (name,))
    count = cur.fetchone()[0]
#    print(count)
    child_name = name + str(count)
#    print(child_name)

    sql = ''' select ID from Topics where name = ?'''
    cur = conn.cursor()
    cur.execute(sql, (name,))
    parent_id = cur.fetchone()[0]
#    print(parent_id)
    
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    create_topic(conn,[None,child_name,parent_id,st])
    
    os.system('/usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic {}'.format(child_name))
               


def export_log(conn):
    sql = ''' SELECT * from Actions '''
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    print (data)
    with open('logdata.csv', 'w', newline='') as f_handle:
        writer = csv.writer(f_handle)
        header = ['Name','Action','Timestamp']
        writer.writerow(header)
        for row in data:
            writer.writerow(row)
    


