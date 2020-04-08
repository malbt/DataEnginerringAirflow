from sqlalchemy_utils.types.pg_composite import psycopg2


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y10
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2010';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y11
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2011';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y12
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2012';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y13
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2013';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y14
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2014';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y15
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2015';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y16
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2016';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y17
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2017';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y18
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2018';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y19
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2019';
        """)
    conn.commit()


def create_table_from_merge():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        create table st_y20
        as
        select  * from stock_sec
        where date_part( 'year',date) ='2020';
        """)
    conn.commit()
