import sqlite3
from sqlite3 import Error
 
 
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    
    return None



def create_table(conn):
    create_table_sql = """ CREATE TABLE IF NOT EXISTS Topics (
                                        ID integer PRIMARY KEY,
                                        Name integer NOT NULL,
                                        Parent_ID integer,
                                        Timestamp text
                                    ); """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def delete_table(conn):
    delete_table_sql = """ DROP TABLE IF EXISTS Topics"""
    try:
        c = conn.cursor()
        c.execute(delete_table_sql)
    except Error as e:
        print(e)       
  
         
def create_topic(conn, topic):
    sql = ''' INSERT INTO Topics(Name,Parent_ID,Timestamp)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, topic)
    return cur.lastrowid


def delete_topic(conn, id):
    cur = conn.cursor()
    sql = ''' DELETE FROM Topics Where ID = ?'''
    cur.execute(sql, (id,))
    sql = ''' DELETE FROM Topics Where Parent_ID = ?'''
    cur.execute(sql, (id,))
    return cur.lastrowid


def merge_topics(conn, id):
    sql = ''' DELETE FROM Topics Where Parent_ID = ?'''
    cur = conn.cursor()
    cur.execute(sql, (id,))
    return cur.lastrowid

