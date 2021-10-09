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
        self.db_conn_id = db_conn_id
    def get_mysql_table(self, mysql_connection, to_update_date_str):
        with mysql_connection.cursor() as cursor:
            sql = """SELECT Brand, max(Date) AS Date,
                     sum(First_Dose_Daily) AS First_Dose_Accumulate,
                     sum(Second_Dose_Daily) AS Second_Dose_Accumulate 
                     FROM covid19_vaccination 
                     WHERE Date <= '{}'
                     GROUP BY (Brand)""".format(to_update_date_str)

            cursor.execute(sql)
            result=cursor.fetchall()
        
        return result

    def get_daily_result(self, vacc_table, to_update_date, mysql_accu_dict):
        to_update_vacc = vacc_table[vacc_table['Date'] > to_update_date]
        to_update_vacc['fd'] = to_update_vacc.sort_values('Date').groupby('Brand')['First_Dose_Accumulate'].shift(1)
        to_update_vacc['sd'] = to_update_vacc.sort_values('Date').groupby('Brand')['Second_Dose_Accumulate'].shift(1)
        to_update_brands = list(to_update_vacc[to_update_vacc['fd'].isna()]['Brand'].unique())
        
        for items in mysql_accu_dict:
            if items['Brand'] in to_update_brands:
                to_update_vacc['fd'][to_update_vacc['Brand'] == items['Brand']] = to_update_vacc['fd'][to_update_vacc['Brand'] == items['Brand']].fillna(items['First_Dose_Accumulate'])
                to_update_vacc['sd'][to_update_vacc['Brand'] == items['Brand']] = to_update_vacc['sd'][to_update_vacc['Brand'] == items['Brand']].fillna(items['Second_Dose_Accumulate'])
                to_update_brands.remove(items['Brand'])

        if len(to_update_brands) != 0:
            for brand in to_update_brands:
                to_update_vacc['fd'][to_update_vacc['Brand'] == brand] = to_update_vacc['fd'][to_update_vacc['Brand'] == brand].fillna(0)
                to_update_vacc['sd'][to_update_vacc['Brand'] == brand] = to_update_vacc['sd'][to_update_vacc['Brand'] == brand].fillna(0)

        to_update_vacc['First_Dose_Daily'] = to_update_vacc['First_Dose_Accumulate'] - to_update_vacc['fd']
        to_update_vacc['Second_Dose_Daily'] = to_update_vacc['Second_Dose_Accumulate'] - to_update_vacc['sd']
        to_update_vacc['Total_Vaccinated_Daily'] = to_update_vacc['First_Dose_Daily'] + to_update_vacc['Second_Dose_Daily']
        to_update_vacc = to_update_vacc.drop(labels=['fd','sd'],axis=1)

        return to_update_vacc

    def execute(self, context):
        """
        Execution function of UpdateVaccTableOperator
        """
        #Create MySqlHook to connect to MySql
        mysql_hook = MySqlHook(conn_name_attr = self.db_conn_id)
        mysql_connection=pymysql.connect(host="127.0.0.1",user='root',password='password',db='airflow',cursorclass=pymysql.cursors.DictCursor)

        #Create Pandas dataframe for 
        vacc_table = LoadTableFunctions.get_vaccination_table("https://covid-19.nchc.org.tw/api/covid19?CK=covid-19@nchc.org.tw&querydata=2004", type = 'accumulated')   
        vacc_table['Date'] = pd.to_datetime(vacc_table['Date'])


        latest_record = datetime.strptime(context['task_instance'].xcom_pull(task_ids = 'Check_latest_vacc', key = 'latest'), '%Y-%m-%d')
        to_update_date = datetime(latest_record.year, latest_record.month, latest_record.day) - timedelta(3)
        to_update_date_str = datetime.strftime(to_update_date, "%Y-%m-%d")

        mysql_accu_dict = self.get_mysql_table(mysql_connection, to_update_date_str)

        to_update_vacc = self.get_daily_result(vacc_table, to_update_date, mysql_accu_dict)
       
        sql_insert = """INSERT INTO covid19_vaccination (Date,Brand,First_Dose_Daily,Second_Dose_Daily,Total_Vaccinated_Daily) VALUES """

        for values in to_update_vacc.values:    
            sql_insert = sql_insert + "('{}','{}',{},{},{}),".format(values[2], values[3], values[7], values[8], values[9])

        sql_insert = sql_insert[:-1] + " ON DUPLICATE KEY UPDATE First_Dose_Daily = VALUES(First_Dose_Daily), Second_Dose_Daily = VALUES(Second_Dose_Daily), Total_Vaccinated_Daily = VALUES(Total_Vaccinated_Daily)"

        if to_update_vacc.shape[0] > 0:
            mysql_hook.run(sql_insert)
            self.log.info("Update {} rows into Covid19_Vaccination table".format(to_update_vacc.shape[0]))
        else:
            self.log.info("There is nothing to update for Covid19_Vaccination table")    
            

