from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import LoadTableFunctions

class LoadVaccOperator(BaseOperator):
    """
    LoadVaccOperator loads vaccination table data into database
    """

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 db_conn_id="mysql_default",
                 *args, **kwargs):

        super(LoadVaccOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        
    def execute(self, context):
        """
        Execution function of LoadCasesOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)

        #Create Pandas dataframe from Taiwan NCHC
        self.log.info("Retrieving original data")
        vacc_table = LoadTableFunctions.get_vaccination_table("https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=2004")
        self.log.info("Finish retrieving original data")

        #Initiate SQL insert command
        self.log.info("Start writing vaccination table to MySql")
        sql_insert = """INSERT INTO covid19_vaccination (Date,Brand,First_Dose_Daily,Second_Dose_Daily) VALUES """
        
        #Iterate through table to be inserted 
        for values in vacc_table.values:    
           sql_insert = sql_insert + "('{}','{}',{},{}),".format(values[2], values[3], values[7], values[8])

        #Run SQL command using MysqlHook
        mysql_hook.run(sql_insert[:-1])

        #Log insertion information
        self.log.info("Finish loading vaccination table")