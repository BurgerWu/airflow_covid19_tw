#import libraries
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd
import pymysql.cursors
from helpers import LoadTableFunctions

class UpdateVaccTableOperator(BaseOperator):
    """
    UpdateVaccTableOperator loads vaccination table data into database
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(UpdateVaccTableOperator, self).__init__(*args, **kwargs)
        #Store attributes in class
        self.db_conn_id = db_conn_id

    def execute(self, context):
        """
        Execution function of UpdateVaccTableOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)
        mysql_connection=pymysql.connect(host="127.0.0.1",user='root',password='password',db='airflow',cursorclass=pymysql.cursors.DictCursor)

        #Create Pandas dataframe from Taiwan NCHC
        vacc_table = LoadTableFunctions.get_vaccination_table("https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=2004", type = 'accumulated')   
        vacc_table['Date'] = pd.to_datetime(vacc_table['Date'])

        #Get latest record by retrieving xcom record from other operator
        #Here we will update from 3 days before latest record to ensure any updated data is tracked
        latest_record = datetime.strptime(context['task_instance'].xcom_pull(task_ids = 'Check_latest_vacc', key = 'latest'), '%Y-%m-%d')
        to_update_date = datetime(latest_record.year, latest_record.month, latest_record.day) - timedelta(3)
        to_update_date_str = datetime.strftime(to_update_date, "%Y-%m-%d")
        
        #Get accumulated data of each vaccine from Mysql
        mysql_accu_dict = LoadTableFunctions.get_mysql_vacc_accumulated_dict(mysql_connection, to_update_date_str)

        #Apply loadtablefunction to transform into daily result
        to_update_vacc = LoadTableFunctions.transform_to_daily_result(vacc_table, to_update_date, mysql_accu_dict)
       
        #Initiate SQL insert command
        sql_insert = """INSERT INTO covid19_vaccination (Date,Brand,First_Dose_Daily,Second_Dose_Daily) VALUES """

        #Iterate through table to be updated 
        for values in to_update_vacc.values:    
            
            #Append insert segment into original SQL command
            sql_insert = sql_insert + "('{}','{}',{},{}),".format(values[2], values[3], values[7], values[8])

        #Update daily data if needed
        sql_insert = sql_insert[:-1] + " ON DUPLICATE KEY UPDATE First_Dose_Daily = VALUES(First_Dose_Daily), Second_Dose_Daily = VALUES(Second_Dose_Daily)"

        #Log update information and run SQL command
        if to_update_vacc.shape[0] > 0:
            mysql_hook.run(sql_insert)
            self.log.info("Update {} rows into Covid19_Vaccination table".format(to_update_vacc.shape[0]))
        else:
            self.log.info("There is nothing to update for Covid19_Vaccination table")    
            

