#import libraries
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd
import pymysql.cursors

class DataQualityCheckOperator(BaseOperator):
    """
    DataQualityCheckOperator checks if data is properly loaded or updated in database
    """
    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 loadorupdate = '',
                 table = '',
                 *args, **kwargs):

        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        #Store attributes in class
        self.db_conn_id = db_conn_id
        self.loadorupdate = loadorupdate
        self.table = table
    def execute(self, context):
        """
        Execution function of UpdateVaccTableOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create dictionary for searching date column
        date_dict = {"cases":"Date_Confirmation",
                     "suspects": "Date_Reported",
                     "vaccination": "Date"}

        #Check if input is valid
        if self.loadorupdate not in ["load","update"]:
            raise ValueError("Input loadorupdate should contain either 'load' or 'update'")
        if self.table not in ['cases','suspects','vaccination']:
            raise ValueError("Input table should be 'cases', 'suspects' or 'vaccination'")

        #Check rows of table after loading if it's data quality check for load operators
        if self.loadorupdate == 'load':
            shape = mysql_hook.get_first("SELECT count(*) FROM {}".format('covid19_' + self.table))[0]
            if shape <= 0:
                raise ValueError("Data quality check shows that no data can be retrieved from table {}".format('covid19_' + self.table))
            else:
                self.log.info("Table {} has passed data quality check and been correctly loaded into database".format('covid19_' + self.table))
        else:   
            #Compare latest record of table after updating if it's data quality check for update operators
            latest_sql = mysql_hook.get_first("SELECT max({}) FROM {}".format(date_dict[self.table],'covid19_' + self.table))[0]
            latest_xcom = datetime.strptime(context['task_instance'].xcom_pull(task_ids = 'Check_latest_' + self.table, key = 'latest_' + self.table), '%Y-%m-%d').date()
            if latest_sql < latest_xcom:
                raise ValueError("Data quality check shows that table {} in database is not correctly updated.".format('covid19_' + self.table))
            else:
                self.log.info("Table {} has passed data quality check and been correctly updated into database".format('covid19_' + self.table))


