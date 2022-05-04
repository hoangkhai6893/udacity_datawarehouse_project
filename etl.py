import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import os

def load_staging_tables(cur, conn):
    """
    Description: Load the staging table then copy data declared on sql_queries script
    Input : cur,conn
    Output: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: Insert data to the table declared on sql_queries script
    Input : cur,conn
    Output: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def get_app_file_path(file):
    
    """
    Description: Return the absolute path of the app's files. They should be in the same folder as this py file.
    Input : String name of file
    Output: String name of file_path
    """
    folder,_ = os.path.split(__file__)
    file_path = os.path.join(folder,file)
    return file_path

def main():
    """
    Description: Load configuration from config file, connect Database S3 to copy and insert data.
    """
    config = configparser.ConfigParser()
    filename = 'dwh.cfg'
    config.read(get_app_file_path(filename))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()