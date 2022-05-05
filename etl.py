import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import os

def load_staging_tables(cur, conn):
    """
    Description: Load the staging table then copy data declared on sql_queries script
    Input : 
        - cur : reference to connected database
        - conn : contain parameters (host,port,dbname,user,password) to connect the database
    Output: log_data to staging events table,song_data to staging songs table
    """
    for query in copy_table_queries:
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('Processed OK.')
        
    print('All files COPIED OK.')

def insert_tables(cur, conn):
    """
    Description: Insert data to the table declared on sql_queries script
    Input :
        - cur : reference to connected database
        - conn : contain parameters (host,port,dbname,user,password) to connect the database
    Output: Data insert from staging table to fact tables and dimension tables.
    """
    print("Start inserting data...")
    for query in insert_table_queries:
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('Processed OK.')
    print('All files INSERTED OK.')
    
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
    Input : None 
    Output: All input data processed in DB tables
    """
    print("Begin process execute ETL")
    config = configparser.ConfigParser()
    filename = 'dwh.cfg'
    config.read(get_app_file_path(filename))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established OK.")

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("Load and Insert data into table is DONE")

    conn.close()


if __name__ == "__main__":
    main()