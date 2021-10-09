from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import pandas as pd

class UpdateCasesTableOperator(BaseOperator):
    """
    UpdateCasesTableOperator loads vaccination table data into database
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(UpdateCasesTableOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id

    def execute(self, context):
        """
        Execution function of LoadCasesOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create Pandas dataframe for 
        case_table = pd.read_json('https://od.cdc.gov.tw/eic/Day_Confirmation_Age_County_Gender_19CoV.json')
        case_table.iloc[:,1] = pd.to_datetime(case_table.iloc[:,1])
        sql_insert = """INSERT INTO covid19_cases (Date_Confirmation, County_Living, Gender, Imported, Age_Group, Number_of_Confirmed_Cases) VALUES """
        
        latest_record = datetime.strptime(context['task_instance'].xcom_pull(task_ids = 'Check_latest_case', key = 'latest'), '%Y-%m-%d')
        
        to_update_case = case_table[case_table.iloc[:,1] > datetime(latest_record.year, latest_record.month, latest_record.day)]
        
        for values in to_update_case.values:    
            sql_insert = sql_insert + "('{}','{}','{}','{}','{}',{}),".format(values[1],values[2],values[4],values[5],values[6],values[7])

        if to_update_case.shape[0] > 0:
            mysql_hook.run(sql_insert[:-1])
            self.log.info("Update {} rows into Covid19_Daily_Cases table".format(to_update_case.shape[0]))
        else:
            self.log.info("There is nothing to update for Covid19_Daily_Cases table")    
            
        self.log.info("Finish loading vaccination table")
