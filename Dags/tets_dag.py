from datetime import timedelta
import airflow
from airflow import DAG
from airflow.example_dags.example_complex import default_args
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from flask import Flask, render_template

# Step 1: importing Modules
# Step2: Default arguments, define default and DAG-specific arguments

default_args = {
    'owner': 'maleda',
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(2020/04/02),
    'depends_on_past': False,
    'email': ['mal.bt27@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # if task fails, retry it once after waiting, min 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=6),

}
# Step 3: Instantiate a DAG
dag = DAG(
    'mytestdag',
    default_args=default_args,
    description='A simple dag ',
    # continue to run DAG once per day
    schedule_interval=timedelta(days=1),

)
# Step 4: Tasks, lay out all the task in the workflow
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)
t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag
)


def blog():
    print("This better work!")
    return


blog = PythonOperator(
    task_id='blog',
    depends_on_past=False,
    python_collable=blog,
)
# Step 5: Setting up Dependencies
t1 >> [t2 >> blog]