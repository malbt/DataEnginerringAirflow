from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

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
