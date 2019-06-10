import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''This function drops all tables in Redshift database'''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''This function creates all tables in Redshift database'''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''This function reads dwh.cfg and connects to Redshift cluster to drop and create
    tables.'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('CONNECTED')
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()