from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class LoadSuspectsOperator(BaseOperator):
    """
    LoadSuspectsOperator loads suspects table data into database
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(LoadSuspectsOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        
    def execute(self, context):
        """
        Execution function of LoadSusoectsOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create Pandas dataframe for 
        suspect_table = pd.read_csv('https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv')
        sql_insert = """INSERT INTO covid19_suspects (通報日, 法定監測送驗, 居家檢疫送驗, 擴大傳染病通報, Total) VALUES """
        
        for values in suspect_table.values:    
           sql_insert = sql_insert + "('{}',{},{},{},{}),".format(values[0],values[1],values[2],values[3],values[4])

        mysql_hook.run(sql_insert[:-1])

        self.log.info("Finish loading suspect table in MySql")