from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd

class UpdateSuspectsTableOperator(BaseOperator):
    """
    UpdateSuspectsTableOperator loads vaccination table data into database
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(UpdateSuspectsTableOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id

    def execute(self, context):
        """
        Execution function of LoadSuspectsOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create Pandas dataframe for 
        suspect_table = pd.read_csv('https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv')
        suspect_table['Date_Reported'] = pd.to_datetime(suspect_table['Date_Reported'])
        sql_insert = """INSERT INTO covid19_suspects (Date_Reported, Reported_Covid19, Reported_Home_Quarantine, Reported_Enhanced_Surveillance, Total_Reported) VALUES """
        
        latest_record = datetime.strptime(context['task_instance'].xcom_pull(task_ids = 'Check_latest_suspect', key = 'latest'), '%Y-%m-%d')
        
        to_update_suspect = suspect_table[suspect_table['Date_Reported'] > datetime(latest_record.year, latest_record.month, latest_record.day) - timedelta(3)]
        
        for values in to_update_suspect.values:    
           sql_insert = sql_insert + "('{}',{},{},{},{}),".format(values[0],values[1],values[2],values[3],values[4])

        sql_insert = sql_insert[:-1] + " ON DUPLICATE KEY UPDATE Reported_Covid19 = VALUES(Reported_Covid19), Reported_Home_Quarantine = VALUES(Reported_Home_Quarantine), Reported_Enhanced_Surveillance = VALUES(Reported_Enhanced_Surveillance), Total_Reported = VALUES(Total_Reported)"

        if to_update_suspect.shape[0] > 0:
            mysql_hook.run(sql_insert)
            self.log.info("Update {} rows into Covid19_Daily_Suspects table".format(to_update_suspect.shape[0]))
        else:
            self.log.info("There is nothing to update for Covid19_Daily_Suspects table")    
            