from datetime import timedelta, datetime

import matplotlib
import psycopg2
import yfinance as yf
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from alembic.ddl import postgresql
from sqlalchemy import create_engine, engine

default_args = {
    'owner': 'maleda',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['mal.bt27@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='yahoo_data_git',
    default_args=default_args,
    description='Tesla sec from yahoo',
    schedule_interval=timedelta(days=1),
)


def connect_pst():
    psycopg2.connect("host=localhost dbname=airflow_test user=postgres")


def get_tes_yahoo():
    tsl_df = yf.download('TSLA')
    infile = tsl_df.to_csv("/Users/mtessema/Desktop/PY/TSLAs.csv", index=True)
    return infile


def create_yahoo_tbl():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS tsyahoo;
        CREATE TABLE tsyahoo(
            Date text,
            Open text,
            High text,
            Low text,
            Close text,
            Adj_Close text,
            Volume text
            )
    """)
    conn.commit()


def insert_csv_yahoo():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/PY/TSLAs.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'tsyahoo', sep=',')
        conn.commit()


def change_data_type_tsyahoo():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table tsyahoo
    alter column "date" type date using ("date"::text::date)""")
    conn.commit()


def create_tbl_edgar():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists edgar;
        CREATE TABLE edgar(
        secname text,
        date text

    )
    """)
    conn.commit()


def insert_csv_edgar():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/TeslaSecED.csv/EDGARf.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'edgar', sep=',')
        conn.commit()


def clean_data_type_edgar():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table edgar
    alter column "date" type date using ("date"::text::date)""")
    conn.commit()


def create_join_tbl():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
    drop table if exists tesla_st;
    create table tesla_st
        as
        select sya.date, open,close,high, low,adj_close,volume, edgar.secname from  tsyahoo as sya
        full outer join edgar
        on sya.date = edgar.date;

    """)
    conn.commit()


def create_table_from_merge1():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y10;
        create table st_y10
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2010';
        """)
    conn.commit()


def create_table_from_merge2():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y11;
        create table st_y11
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2011';
        """)
    conn.commit()


def create_table_from_merge3():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y12;
        create table st_y12
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2012';
        """)
    conn.commit()


def create_table_from_merge4():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y13;
        create table st_y13
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2013';
        """)
    conn.commit()


def create_table_from_merge5():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y14;
        create table st_y14
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2014';
        """)
    conn.commit()


def create_table_from_merge6():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y15;
        create table st_y15
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2015';
        """)
    conn.commit()


def create_table_from_merge7():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y16;
        create table st_y16
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2016';
        """)
    conn.commit()


def create_table_from_merge8():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y17;
        create table st_y17
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2017';
        """)
    conn.commit()


def create_table_from_merge9():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y18;
        create table st_y18
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2018';
        """)
    conn.commit()


def create_table_from_merge10():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y19;
        create table st_y19
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2019';
        """)
    conn.commit()


def create_table_from_merge11():
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    cur = conn.cursor()
    cur.execute("""
        drop table if exists st_y20;
        create table st_y20
        as
        select  * from tesla_st
        where date_part( 'year',date) ='2020';
        """)
    conn.commit()


t1 = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

t2 = PythonOperator(
    task_id='get_tes_yahoo',
    provide_context=False,
    python_callable=get_tes_yahoo,
    dag=dag,
)

t3 = PythonOperator(
    task_id='create_tbl_yahoo',
    provide_context=False,
    python_callable=create_yahoo_tbl,
    dag=dag,
)

t4 = PythonOperator(
    task_id='insert_csv_yahoo',
    provide_context=False,
    python_callable=insert_csv_yahoo,
    dag=dag,
)

t5 = PythonOperator(
    task_id='change_data_type_tsyahoo',
    provide_context=False,
    python_callable=change_data_type_tsyahoo,
    dag=dag,
)

t6 = PythonOperator(
    task_id='create_table_edgar',
    provide_context=False,
    python_callable=create_tbl_edgar,
    dag=dag,
)

t7 = PythonOperator(
    task_id='insert_csv_edgar',
    provide_context=False,
    python_callable=insert_csv_edgar,
    dag=dag,
)

t8 = PythonOperator(
    task_id='clean_data_type_edgar',
    provide_context=False,
    python_callable=clean_data_type_edgar,
    dag=dag,
)
t9 = PythonOperator(
    task_id='create_join_tbl',
    provide_context=False,
    python_callable=create_join_tbl,
    dag=dag,
)
t10 = PythonOperator(
    task_id='create_table_from_merge1',
    provide_context=False,
    python_callable=create_table_from_merge1,
    dag=dag,
)
t11 = PythonOperator(
    task_id='create_table_from_merge2',
    provide_context=False,
    python_callable=create_table_from_merge2,
    dag=dag,
)
t12 = PythonOperator(
    task_id='create_table_from_merge3',
    provide_context=False,
    python_callable=create_table_from_merge3,
    dag=dag,
)
t13 = PythonOperator(
    task_id='create_table_from_merge4',
    provide_context=False,
    python_callable=create_table_from_merge4,
    dag=dag,
)
t14 = PythonOperator(
    task_id='create_table_from_merge5',
    provide_context=False,
    python_callable=create_table_from_merge5,
    dag=dag,
)
t15 = PythonOperator(
    task_id='create_table_from_merge6',
    provide_context=False,
    python_callable=create_table_from_merge6,
    dag=dag,
)
t16 = PythonOperator(
    task_id='create_table_from_merge7',
    provide_context=False,
    python_callable=create_table_from_merge7,
    dag=dag,
)
t17 = PythonOperator(
    task_id='create_table_from_merge8',
    provide_context=False,
    python_callable=create_table_from_merge8,
    dag=dag,
)
t18 = PythonOperator(
    task_id='create_table_from_merge9',
    provide_context=False,
    python_callable=create_table_from_merge9,
    dag=dag,
)
t19 = PythonOperator(
    task_id='create_table_from_merge10',
    provide_context=False,
    python_callable=create_table_from_merge10,
    dag=dag,
)
t20 = PythonOperator(
    task_id='create_table_from_merge11',
    provide_context=False,
    python_callable=create_table_from_merge11,
    dag=dag,
)

t1
t2 >> t3 >> t4 >> t5
t6 >> t7 >> t8
t9.set_upstream(t5)
t9.set_upstream(t8)

t10.set_upstream(t9)
t11.set_upstream(t9)
t12.set_upstream(t9)
t13.set_upstream(t9)
t14.set_upstream(t9)
t15.set_upstream(t9)
t16.set_upstream(t9)
t17.set_upstream(t9)
t18.set_upstream(t9)
t19.set_upstream(t9)
t20.set_upstream(t9)


def create_chart_10_years():
    import plotly.graph_objects as go
    import pandas as pd

    t_df = pd.read_csv('/Users/mtessema/Desktop/PY/TSLAs.csv')

    fig = go.Figure(data=[go.Candlestick(x=t_df['Date'],
                                         open=t_df['Open'],
                                         high=t_df['High'],
                                         low=t_df['Low'],
                                         close=t_df['Close'])])

    fig.show()
    # fig.savefig('tests.png')


t22 = PythonOperator(
    task_id='create_chart_10_years',
    provide_context=False,
    python_callable=create_chart_10_years,
    dag=dag,
)


def chart_yr10():
    import plotly.graph_objects as go
    import pandas as pd

    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y10"
    dt10_df = pd.read_sql_query(sql, conn)
    conn = None
    fig = go.Figure(data=[go.Candlestick(x=dt10_df['date'],
                                         open=dt10_df['open'],
                                         high=dt10_df['high'],
                                         low=dt10_df['low'],
                                         close=dt10_df['close'])])

    fig.show()
    # fig.savefig('first-sec-2010-08-04--08-19.png')


t23 = PythonOperator(
    task_id='chart_yr10',
    provide_context=False,
    python_callable=chart_yr10,
    dag=dag,
)


def chart_yr11():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y11"
    dt11_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt11_df['date'],
                                         open=dt11_df['open'],
                                         high=dt11_df['high'],
                                         low=dt11_df['low'],
                                         close=dt11_df['close'])])

    fig.show()
    # fig.savefig('year-2011-chart.png')


t24 = PythonOperator(
    task_id='chart_yr11',
    provide_context=False,
    python_callable=chart_yr11,
    dag=dag,
)


def chart_yr12():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y12"
    dt12_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt12_df['date'],
                                         open=dt12_df['open'],
                                         high=dt12_df['high'],
                                         low=dt12_df['low'],
                                         close=dt12_df['close'])])

    fig.show()
    # fig.savefig('year-2012-chart.png')


t25 = PythonOperator(
    task_id='chart_yr12',
    provide_context=False,
    python_callable=chart_yr12,
    dag=dag,
)


def chart_yr13():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y13"
    dt13_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt13_df['date'],
                                         open=dt13_df['open'],
                                         high=dt13_df['high'],
                                         low=dt13_df['low'],
                                         close=dt13_df['close'])])

    fig.show()
    # fig.savefig('year-2013-chart.png')


t26 = PythonOperator(
    task_id='chart_yr13',
    provide_context=False,
    python_callable=chart_yr13,
    dag=dag,
)


def chart_yr14():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y14"
    dt14_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt14_df['date'],
                                         open=dt14_df['open'],
                                         high=dt14_df['high'],
                                         low=dt14_df['low'],
                                         close=dt14_df['close'])])

    fig.show()
    # fig.savefig('year-2014-chart.png')


t27 = PythonOperator(
    task_id='chart_yr14',
    provide_context=False,
    python_callable=chart_yr14,
    dag=dag,
)


def chart_yr15():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y15"
    dt15_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt15_df['date'],
                                         open=dt15_df['open'],
                                         high=dt15_df['high'],
                                         low=dt15_df['low'],
                                         close=dt15_df['close'])])

    fig.show()
    # fig.savefig('year-2015-chart.png')


t28 = PythonOperator(
    task_id='chart_yr15',
    provide_context=False,
    python_callable=chart_yr15,
    dag=dag,
)


def chart_yr16():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y16"
    dt16_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt16_df['date'],
                                         open=dt16_df['open'],
                                         high=dt16_df['high'],
                                         low=dt16_df['low'],
                                         close=dt16_df['close'])])

    fig.show()
    # fig.savefig('year-2016-chart.png')


t29 = PythonOperator(
    task_id='chart_yr19',
    provide_context=False,
    python_callable=chart_yr16,
    dag=dag,
)


def chart_yr17():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y17"
    dt17_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt17_df['date'],
                                         open=dt17_df['open'],
                                         high=dt17_df['high'],
                                         low=dt17_df['low'],
                                         close=dt17_df['close'])])

    fig.show()
    # fig.savefig('year-2017-chart.png')


t30 = PythonOperator(
    task_id='chart_yr17',
    provide_context=False,
    python_callable=chart_yr17,
    dag=dag,
)


def chart_yr18():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y18"
    dt18_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt18_df['date'],
                                         open=dt18_df['open'],
                                         high=dt18_df['high'],
                                         low=dt18_df['low'],
                                         close=dt18_df['close'])])

    fig.show()
    # fig.savefig('year-2018-chart.png')


t31 = PythonOperator(
    task_id='chart_yr18',
    provide_context=False,
    python_callable=chart_yr18,
    dag=dag,
)


def chart_yr19():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y19"
    dt19_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt19_df['date'],
                                         open=dt19_df['open'],
                                         high=dt19_df['high'],
                                         low=dt19_df['low'],
                                         close=dt19_df['close'])])

    fig.show()
    # fig.savefig('year-2019-chart.png')


t32 = PythonOperator(
    task_id='chart_yr19',
    provide_context=False,
    python_callable=chart_yr19,
    dag=dag,
)


def chart_yr20():
    import plotly.graph_objects as go
    import pandas as pd
    conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    sql = "select  * from st_y20"
    dt20_df = pd.read_sql_query(sql, conn)
    fig = go.Figure(data=[go.Candlestick(x=dt20_df['date'],
                                         open=dt20_df['open'],
                                         high=dt20_df['high'],
                                         low=dt20_df['low'],
                                         close=dt20_df['close'])])

    fig.show()
    # fig.savefig('year-2020-chart.png')


t33 = PythonOperator(
    task_id='chart_yr20',
    provide_context=False,
    python_callable=chart_yr20,
    dag=dag,
)

t22.set_upstream(t2)
t23.set_upstream(t10)
t24.set_upstream(t11)
t25.set_upstream(t12)
t26.set_upstream(t13)
t27.set_upstream(t14)
t28.set_upstream(t15)
t29.set_upstream(t16)
t30.set_upstream(t17)
t31.set_upstream(t18)
t32.set_upstream(t19)
t33.set_upstream(t20)
