#import libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
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
dag = DAG('test_dag_mysql',
          default_args = default_args,
          description = 'Test with Airflow',
          schedule_interval = '@hourly')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Create create_tables task
create_tables = MySqlOperator(
    task_id = 'Create_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, height float, weight float)")

insert_data = MySqlOperator(
    task_id = 'Insert_data',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = "INSERT INTO test(id, height, weight) VALUES(1, 173, 86.4)")


#Create end_operator task
end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

#Schedule sequential relationship between tasks
start_operator \
>> create_tables \
>> insert_data \
>> end_operator
