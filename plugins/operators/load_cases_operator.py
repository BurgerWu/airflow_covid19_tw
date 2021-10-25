#import libraries
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from helpers import LoadTableFunctions

class LoadCasesOperator(BaseOperator):
    """
    LoadCasesOperator loads cases table data into database
    """
    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(LoadCasesOperator, self).__init__(*args, **kwargs)
        #Store attributes in class
        self.db_conn_id = db_conn_id
        
    def execute(self, context):
        """
        Execution function of LoadCasesOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create Pandas dataframe from Taiwan CDC
        case_table = pd.read_json('https://od.cdc.gov.tw/eic/Day_Confirmation_Age_County_Gender_19CoV.json')
        case_table = LoadTableFunctions.translate_case_column(case_table)
        
        #Initiate SQL insert command
        sql_insert = """INSERT INTO covid19_cases (Date_Confirmation, County_Living, Gender, Imported, Age_Group, Number_of_Confirmed_Cases) VALUES """
        
        #Iterate through table to be inserted 
        for values in case_table.values: 

            #Append insert segment into original SQL command   
            sql_insert = sql_insert + "('{}','{}','{}','{}','{}',{}),".format(values[1],values[2],values[4],values[5],values[6],values[7])

        #Run SQL command using MysqlHook
        mysql_hook.run(sql_insert[:-1])

        #Log insertion information
        self.log.info("Finish loading case table in MySql")