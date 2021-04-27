import configparser
import logging

import psycopg2

from src.util.sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops staging, fact and dimension tables
    :param cur: database connection cursor
    :param conn: database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates staging, fact and dimension tables
    :param cur: database connection cursor
    :param conn: database connection
    """
    for query in create_table_queries:
        print(query) # to see the failing query
        cur.execute(query)
        conn.commit()


def main():
    """
    Opens a psycopg2 connection to redshift and `create_tables`
    """
    config = configparser.ConfigParser()
    config.read(r'dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    logging.info('tables are created')

    conn.close()


if __name__ == "__main__":
    main()
