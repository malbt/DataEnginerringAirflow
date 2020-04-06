from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ROWS,
from airflow.hooks.mysql_hook import MySqlHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['mal.bt27@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=6),
}

dag = DAG(
    dag_id='stk_data',
    default_args=default_args,
    description='Tesla stock data from API',
    schedule_interval=timedelta(days=1),
)
# get stock data
get_data = BashOperator(
    task_id='get_kaggle_api',
    bash_command='kaggle datasets download -d timoboz/tesla-stock-data-from-2010-to-2020',
    dag=dag,
)

# test connection to postgres
def connect_pst():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")

# connect and create table
def create_tbl():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE sec(
        SECFormName text,
        Description text,
        Date text
    )
    """)
    conn.commit()

# connect and insert
def insert_csv():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/PY/TeslaSec.csv", 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'sec', sep=',')
        conn.commit()


t1 = PythonOperator(
    task_id='connect_pst',
    provide_context=False,
    python_callable=connect_pst,
    dag=dag,
)

t2 = PythonOperator(
    task_id='create_tbl',
    provide_context=False,
    python_callable=create_tbl,
    dag=dag,
)
t3 = PythonOperator(
    task_id='insert_csv',
    provide_context=False,
    python_callable=insert_csv,
    dag=dag,
)


t2 >> t3