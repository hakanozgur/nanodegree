import configparser
import logging

import psycopg2

from src.util.sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Gets data from s3 into redshift staging table

    :param cur: database connection cursor
    :param conn: database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Takes the data from staging table, transforms and loads into fact and dimension tables

    :param cur: database connection cursor
    :param conn: database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads data from aws s3 to aws redshift
    """
    config = configparser.ConfigParser()
    config.read(r'dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    logging.info('staging table loaded')

    insert_tables(cur, conn)
    logging.info('staging data inserted to fact and dimension tables')

    conn.close()


if __name__ == "__main__":
    main()
