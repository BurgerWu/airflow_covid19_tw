#import libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.test_operator import TestOperator

#Create default argument for dag
default_args = {
    'owner': 'burger_wu',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}

#Create DAG with daily schedule_interval and start data as now
dag = DAG('test_dag_postgresql',
          default_args = default_args,
          description = 'Test with Airflow',
          schedule_interval = '@hourly')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Drop tables task
drop_tables = PostgresOperator(
    task_id = 'Drop_tables',
    dag = dag,
    postgres_conn_id = "postgres_default",
    sql = "DROP TABLE test")

#Create create_tables task
create_tables = PostgresOperator(
    task_id = 'Create_tables',
    dag = dag,
    postgres_conn_id = "postgres_default",
    sql = "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, height float, weight float)")

insert_data = TestOperator(
    task_id = 'Insert_data',
    dag = dag,
    postgres_conn_id = "postgres_default",
    value = [1, 173, 86.4])

def printxcom(**context):
    latest = context['task_instance'].xcom_pull(task_ids='Insert_data', key = 'key')
    return latest

print_xcom = PythonOperator(
    task_id='print_xcom',
    python_callable = printxcom,
    provide_context = True,
    dag = dag)

#Create end_operator task
end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

#Schedule sequential relationship between tasks
start_operator \
>> drop_tables \
>> create_tables \
>> insert_data \
>> print_xcom \
>> end_operator
