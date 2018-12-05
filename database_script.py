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
        


def create_topic(conn, topic):
    sql = ''' INSERT INTO Topics(ID,Name,Parent_ID,Timestamp)
              VALUES(?,?,?,?) '''
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






if __name__ == '__main__':
    conn = create_connection("Topic_DB.db")
    with conn:
        create_table(conn)
        topic = ('1','Topic_b','',"3:00PM")
        row = create_topic(conn,topic)
        topic = ('2','Topic_b','',"3:00PM")
        row = create_topic(conn,topic)
        topic = ('3','Topic_b','1',"3:00PM")
        row = create_topic(conn,topic)
        topic = ('4','Topic_b','1',"3:00PM")
        row = create_topic(conn,topic)
        topic = ('5','Topic_b','2',"3:00PM")
        row = create_topic(conn,topic)
        row = merge_topics(conn,1)
