import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: Copy all data on the S3 buckets to the staging tables.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: Read the event data in S3 buckets, and then execute the ingest process for each file and save it to 
    the Redshift database

    Arguments:
        cur: the cursor object.
        conn: connection to the database.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is responsible for running  the ETL pipeline process.
    - Establishes a connection with the raw data in S3 buckets and Redshift database and gets cursor to it.
    - Read the event data in S3 buckets, and then execute the ingest process for each file and save it to 
    the Redshift database
    - Finally, closes the connection.

    Arguments:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()