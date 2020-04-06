from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2

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
def create_tbl_sec():
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
def insert_csv_sec():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/PY/TeslaSec.csv", 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'sec', sep=',')
        conn.commit()


# created stock table
def create_tbl_stock():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE stock(
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


# insert stock csv data
def insert_csv_Stock():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/PY/TSLA.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'stock', sep=',')
        conn.commit()


# change date type for both TABLE
def clean_data_type_sec():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table sec
    alter column "date" type date using ("date"::text::date)""")
    conn.commit()


def clean_data_type_stock():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    cur = conn.cursor()
    cur.execute("""
    alter table stock
    alter column "date" type date using ("date"::text::date)""")
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
    python_callable=create_tbl_sec,
    dag=dag,
)
t3 = PythonOperator(
    task_id='insert_csv',
    provide_context=False,
    python_callable=insert_csv_sec,
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_tbl_stock',
    provide_context=False,
    python_callable=create_tbl_stock,
    dag=dag,
)
t5 = PythonOperator(
    task_id='insert_csv_Stock',
    provide_context=False,
    python_callable=insert_csv_Stock,
    dag=dag,
)
t6 = PythonOperator(
    task_id='clean_data_type_sec',
    provide_context=False,
    python_callable=clean_data_type_sec,
    dag=dag,
)
t7 = PythonOperator(
    task_id='clean_data_type_stock',
    provide_context=False,
    python_callable=insert_csv_Stock,
    dag=dag,
)
t1
t2 >> t3 >> t6
t4 >> t5 >> t7
# merge the two table columns
# SELECT *
# FROM
# sec, stock_dummy
# WHERE
# sec.date = stock_dummy.date;
