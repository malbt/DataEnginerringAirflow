from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

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

get_data = BashOperator(
    task_id='get_kaggle_api',
    bash_command='kaggle datasets download -d timoboz/tesla-stock-data-from-2010-to-2020',
    dag=dag,
)


# create a sql engine
def create_engine():
    mysql_engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost:3306/airflow_db')
    return mysql_engine


create_engine = PythonOperator(
    task_id='create_mysql_engine',
    provide_context=True,
    python_callable=create_engine,
    dag=dag,
)
def load_data():
    query_load_data_infile = 'LOAD DATA INFILE "/Users/mtessema/Desktop/PY/TSLA.csv"' \
                             'INTO TABLE stock' \
                             'FIELDS TERMINATED BY ', ' ' \
                             'ENCLOSED BY ["]'\
                             'LINES TERMINATED BY [\n]' \
                             'IGNORE 1 ROWS;'
    return query_load_data_infile

load_data= PythonOperator(
        task_id='load_data',
        provide_context=True,
        python_callable=load_data,
        dag=dag,
)
"""
tasks
get_data
create_engine >> load_data
"""

"""
only one task has been working so far, 
i am creating separate dags for the tasks to figure out what works best
i haven't been commiting my work because i had to create a separate .py file
because i haven't been able to integrate the file's i wanted in to my airflow, 
i did clone my project inside the airflow dir, but i had to move the file into the dag.
port was right now am having config errors
create_engine() got an unexpected keyword argument 'conf'
i have tried d/t ways to import my data to mysql db, some dags run with no error but at the 
end there is no data

"""

def excel_sec():
    book = xlrd.open_workbook("/Users/mtessema/Desktop/PY/TeslaSec(1).xlsx")
    sheet = book.sheet_by_index(0)
    database = pymysql.connect(host='localhost',
                               user='root',
                               password='zipcoder',
                               db="airflow_db")
    cursor = database.cursor()
    query = "INSERT INTO sec (Date, Description, SEC) Values(%s, %s, %s)"
    for r in range(1, sheet.nrows):
        Date = sheet.cell(r, 2).value
        Description = sheet.cell(r, 1).value
        SEC = sheet.cell(r, 0).value

        Values = (Date, Description, SEC)
        cursor.execute(query, Values)
    cursor.close()
    database.commit()
    database.close()


t1 = PythonOperator(
    task_id='excel_sec',
    provide_context=False,
    python_callable=excel_sec,
    dag=dag,
)
