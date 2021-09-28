from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.check_mysql_record_operator import CheckMySqlRecordOperator
from operators.update_cases_table_operator import UpdateCasesTableOperator

#Create default argument for dag
default_args = {
    'owner': 'burger_wu',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes = 2)
}

dag = DAG('update_cases',
          default_args = default_args,
          description = 'Update covid19 cases table',
          schedule_interval = '@daily')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Check latest record first
check_latest_case = CheckMySqlRecordOperator(
    task_id = 'Check_latest_case',
    dag = dag,
    db_conn_id = "mysql_default",
    table = 'cases')

update_cases = UpdateCasesTableOperator(
    task_id = 'Update_cases_table',
    dag = dag,
    db_conn_id = 'mysql_default'
)

#Create start_operator task
end_operator = DummyOperator(task_id = 'End_execution',  dag = dag)

start_operator >> check_latest_case >> update_cases >> end_operator