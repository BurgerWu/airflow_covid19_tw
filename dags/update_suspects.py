#import libraries
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.check_mysql_record_operator import CheckMySqlRecordOperator
from operators.update_suspects_table_operator import UpdateSuspectsTableOperator
from operators.data_quality_check_operator import DataQualityCheckOperator

#Create default argument for dag
default_args = {
    'owner': 'burger_wu',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes = 2)
}

#Create dag instance
dag = DAG('update_suspects',
          default_args = default_args,
          description = 'Update covid19 suspects table',
          schedule_interval = '@daily')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Check latest record first
check_latest_suspects = CheckMySqlRecordOperator(
    task_id = 'Check_latest_suspects',
    dag = dag,
    db_conn_id = "mysql_default",
    table = 'suspects')

#update suspects table based on latest record
update_suspects = UpdateSuspectsTableOperator(
    task_id = 'Update_suspects_table',
    dag = dag,
    db_conn_id = 'mysql_default'
)

#Perform data quality check for suspects table
check_suspects_update = DataQualityCheckOperator(
    task_id = 'Check_suspects_update',
    dag = dag,
    db_conn_id = 'mysql_default',
    loadorupdate = 'update',
    table = 'suspects')


#Create start_operator task
end_operator = DummyOperator(task_id = 'End_execution',  dag = dag)

start_operator >> check_latest_suspects >> update_suspects >> check_suspects_update >> end_operator