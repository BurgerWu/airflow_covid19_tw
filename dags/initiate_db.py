from datetime import datetime, timedelta
import requests
import re
import pandas as pd
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.load_vacc_operator import LoadVaccOperator
from operators.load_cases_operator import LoadCasesOperator
from operators.load_suspects_operator import LoadSuspectsOperator


#Create default argument for dag
default_args = {
    'owner': 'aritek',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes = 2)
}

dag = DAG('initiate_database',
          default_args = default_args,
          description = 'Initiate database with desired schema',
          schedule_interval = '@once')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Create drop_covid19_cases_tables task
drop_case_tables = MySqlOperator(
    task_id = 'Drop_case_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = "DROP TABLE IF EXISTS covid19_cases")

#Create drop_covid19_suspects_tables task
drop_suspect_tables = MySqlOperator(
    task_id = 'Drop_suspect_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = "DROP TABLE IF EXISTS covid19_suspects")

#Create drop_covid19_vaccination_tables task
drop_vacc_tables = MySqlOperator(
    task_id = 'Drop_vacc_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = "DROP TABLE IF EXISTS covid19_vaccination")

#Create create_covid19_cases_tables task
create_case_tables = MySqlOperator(
    task_id = 'Create_case_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = """CREATE TABLE covid19_cases (
    id SERIAL PRIMARY KEY,
    Date_Confirmation date NOT NULL,
    County_Living text,
    Gender text,
    Imported boolean,
    Age_Group text,
    Number_of_Confirmed_Cases int NOT NULL)""")

#Create create_covid19_suspects_tables task
create_suspect_tables = MySqlOperator(
    task_id = 'Create_suspect_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = """CREATE TABLE covid19_suspects (
    id SERIAL,
    Date_Reported date PRIMARY KEY,
    Reported_Covid19 int,
    Reported_Home_Quarantine int,
    Reported_Enhanced_Surveillance int,
    Total_Reported int NOT NULL)""")

#Create create_covid19_vaccination_tables task
create_vacc_tables = MySqlOperator(
    task_id = 'Create_vacc_tables',
    dag = dag,
    mysql_conn_id = "mysql_default",
    sql = """CREATE TABLE covid19_vaccination (
        Date date,
        Brand VARCHAR(20) NOT NULL,
        First_Dose_Daily int NOT NULL,
        Second_Dose_Daily int NOT NULL,
        Total_Vaccinated_Daily int NOT NULL,
        PRIMARY KEY(Date,Brand))""")

#Insert Daily Cases table
load_case_table = LoadCasesOperator(
    task_id = "Load_daily_cases_table",
    dag = dag,
    db_conn_id = "mysql_default")

#Insert Suspects table
load_suspect_table = LoadSuspectsOperator(
    task_id = "Load_suspect_cases_table",
    dag = dag,
    db_conn_id = "mysql_default")

#Insert Vaccination table
load_vacc_table = LoadVaccOperator(
    task_id = "Load_vaccination_table",
    dag = dag,
    db_conn_id = "mysql_default")

#Create end_operator task
end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

drop_case_tables.set_downstream(create_case_tables)
drop_suspect_tables.set_downstream(create_suspect_tables)
drop_vacc_tables.set_downstream(create_vacc_tables)
create_case_tables.set_downstream(load_case_table)
create_suspect_tables.set_downstream(load_suspect_table)
create_vacc_tables.set_downstream(load_vacc_table)

#Schedule sequential relationship between tasks
start_operator >> [drop_case_tables, drop_suspect_tables, drop_vacc_tables] 
[load_case_table, load_suspect_table, load_vacc_table] >> end_operator
