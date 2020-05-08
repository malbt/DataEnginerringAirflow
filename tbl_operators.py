from airflow.operators.python_operator import PythonOperator

from create_tbls import create_table_from_merge2, create_table_from_merge3, create_table_from_merge4, \
    create_table_from_merge5, create_table_from_merge6, create_table_from_merge11, create_table_from_merge10, \
    create_table_from_merge9, create_table_from_merge8, create_table_from_merge7, create_table_from_merge1
from getdata import dag
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
