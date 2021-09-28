from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime

class CheckMySqlRecordOperator(BaseOperator):
    """
    CheckMySqlRecordOperator checks records in MySql database and returned desired result
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 table = '',
                 *args, **kwargs):

        super(CheckMySqlRecordOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.table = table

    def execute(self, context):
        """
        Execution function of CheckMySqlRecordOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        if self.table in ['cases', 'suspects', 'vaccination']:
            table_of_interest = "covid19_{}".format(self.table)
            if self.table == 'vaccination':
                latest = mysql_hook.get_first("SELECT max(日期) FROM {}".format(table_of_interest))[0]
                latest_str = latest.strftime("%Y-%m-%d")
            elif self.table == 'suspects':
                latest = mysql_hook.get_first("SELECT max(通報日) FROM {}".format(table_of_interest))[0]
                latest_str = latest.strftime("%Y-%m-%d")
            else:
                latest = mysql_hook.get_first("SELECT max(個案研判日) FROM {}".format(table_of_interest))[0]
                latest_str = latest.strftime("%Y-%m-%d")
        else:
            raise ValueError("The input value of table is invalid, you should type either cases, suspects or vaccination")
        
        self.log.info("The latest value for {} is {}".format(table_of_interest, latest_str))
        context['task_instance'].xcom_push(key='latest', value = latest_str)
