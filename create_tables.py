import configparser
import psycopg2
import os
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description:  Delete existing tables to be able to create new tables. 
    Input : cur,conn
    Output : None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: dropping table: " + query)
            print(e)
    print("Tables is successfully dropped .")

def create_tables(cur, conn):
    """
    Description: Create staging and dimensional tables declared on sql_queries script
    Input : cur,conn
    Output : None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: creating table: " + query)
            print(e)
    print("Tables is successfully created .")

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
    Description: Connect to AWS Redshift, create new Database table. Drop old tables and crete new tables.
    Input: dwh.cfg file
    Output: New data tables is created
    """
    print("Running drop and create table process")
    config = configparser.ConfigParser()
    filename = 'dwh.cfg'
    config.read(get_app_file_path(filename))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Drop and Create Table is DONE")

    conn.close()


if __name__ == "__main__":
    main()