#import libraries
import requests
import re 
import pandas as pd
from datetime import datetime, timedelta


class LoadTableFunctions:
    """
    LoadTableFunctions class contains custom functions required for processing covid19 raw data
    """
    def translate_case_column(df):
        """
        This functions translates columns in case table from Chinese to English
        Input: Targer dataframe
        Output: Translated dataframe
        """
        #Define translation dictionary
        county_dict = {
            "彰化縣":"Changhua County",
            "南投縣":"Nantou County",
            "台中市":"Taichung City",
            "新北市":"New Taipei City",
            "桃園市":"Taoyuan City",
            "台南市":"Tainan City",
            "台北市":"Taipei City",
            "新竹市":"Hsinchu City",
            "基隆市":"Keelung County",
            "宜蘭縣":"Yilan County",
            "高雄市":"Kaohsiung City",
            "新竹縣":"Hsinchu County",
            "苗栗縣":"Miaoli County",
            "雲林縣":"Yunlin County",
            "屏東縣":"Pingtung County",
            "花蓮縣":"Hualien County",
            "嘉義市":"Chiayi City",
            "嘉義縣":"Chiayi County",
            "台東縣":"Taitung County",
            "連江縣":"Lienchian County",
            "澎湖縣":"Penghu County"}

        #Define imported dictionary
        imported_dict = {"是":1, "否":0}

        #Define gender dictionary
        gender_dict = {"男":"male","女":"female"}

        #Map county column in raw dataframe with dictionary and fill NA as imported cases
        df['縣市'] = df['縣市'].map(county_dict)
        df['縣市'] = df['縣市'].fillna("Imported")
        
        #Map gender column in raw dataframe with dictionary 
        df['性別'] = df['性別'].map(gender_dict)

        #Map imported column in raw dataframe with dictionary
        df['是否為境外移入'] = df['是否為境外移入'].map(imported_dict)

        #Return translated dataframe
        return df

    def get_vaccination_table(url, type = 'daily'):
        """
        This function retrieves vaccination information from National Center for High-Performance Computing (NCHC) 
        along with the help of text manipulation to get original table.
    
        Input: Target URL
        Output: Latest vaccination table
        """
        #Check if type input is valid, if not, return error message
        if type not in ['daily', 'accumulated']:
            raise ValueError("Input for type should be either daily or accumulated")

        #Pre-defined column names
        column_dict = {'id': 'id', 'a01': 'Country', 'a02': 'Date', 'a03':'Brand', 'a04':'First_Dose_Accumulate', 'a05': 'Second_Dose_Accumulate', 'a06':'Total_Vaccination'}
        brand_dict = {'Oxford\/AstraZeneca': 'AstraZeneca', '高端': 'Medigen', 'BNT': 'BNT', 'Moderna':'Moderna'}

        #Use get method in requests to acquire vaccination data
        vacc_content = requests.get(url)
        vacc_rows = re.findall(r'{([0-9a-zA-Z,:"-\\/高端]*)}', vacc_content.text)
    
        #Initiate data list
        data_list=[]

        #Iterate through rows of vaccination table
        for rows in vacc_rows:
        
            #Initiate dictionary for storing row information
            temp_dict = {}
        
            #Iterate through each object in a row
            for item in rows.split(','):
            
                #Convert column name and retrieve item name
                col_name = column_dict[item.split(':')[0][1:-1]]
                item_to_add = item.split(':')[1][1:-1]
                temp_dict[col_name] = item_to_add
        
            #Do not load information for all brand of vaccine
            if temp_dict['Brand'] != 'ALL':  
                temp_dict['Brand'] = brand_dict[temp_dict['Brand']]      
                data_list.append(temp_dict)    
    
        #Create Pandas dataframe using data list created
        vacc_table = pd.DataFrame(data_list)
    
        #Convert datatype to correct ones
        vacc_table['id'] = vacc_table['id'].astype(int)
        vacc_table['First_Dose_Accumulate'] = vacc_table['First_Dose_Accumulate'].astype(int)
        vacc_table['Second_Dose_Accumulate'] = vacc_table['Second_Dose_Accumulate'].astype(int)
        vacc_table['Total_Vaccination'] = vacc_table['Total_Vaccination'].astype(int)

        if type == 'daily':
            #Calculate daily vaccination counts
            vacc_table['fd'] = vacc_table.sort_values('Date').groupby('Brand')['First_Dose_Accumulate'].shift(1).fillna(0)
            vacc_table['sd'] = vacc_table.sort_values('Date').groupby('Brand')['Second_Dose_Accumulate'].shift(1).fillna(0)
            vacc_table['First_Dose_Daily'] = vacc_table['First_Dose_Accumulate'] - vacc_table['fd']
            vacc_table['Second_Dose_Daily'] = vacc_table['Second_Dose_Accumulate'] - vacc_table['sd']
            vacc_table['Total_Vaccinated_Daily'] = vacc_table['First_Dose_Daily'] + vacc_table['Second_Dose_Daily']
            vacc_table = vacc_table.drop(labels=['fd','sd'],axis=1)
        else:
            #Do not modify if type is accumulated
            pass

        #Return target vaccination table
        return vacc_table

    def get_mysql_vacc_accumulated_dict(mysql_connection, to_update_date_str):
        """
        This function returns accumulated vaccination data before to_update_date
        Input: Mysql connection and to_update_date in string
        Output: Returned result from Mysql
        """
        #Build connection with mysql 
        with mysql_connection.cursor() as cursor:
            sql = """SELECT Brand, max(Date) AS Date,
                     sum(First_Dose_Daily) AS First_Dose_Accumulate,
                     sum(Second_Dose_Daily) AS Second_Dose_Accumulate 
                     FROM covid19_vaccination 
                     WHERE Date <= '{}'
                     GROUP BY (Brand)""".format(to_update_date_str)
            #Run the SQL command and fetch all results
            cursor.execute(sql)
            result=cursor.fetchall()
        
        return result

    def transform_to_daily_result(vacc_table, to_update_date, mysql_accu_dict):
        """
        This function returns the subset of vaccinaion table according to to_update date
        Input: Original vacciantion table, date to update and dictionary of accumulated vaccination data retrieved from mysql 
        Output: Vaccination table required for updating database
        """
        #Retrieve subset of dataframe after to_update_date
        to_update_vacc = vacc_table[vacc_table['Date'] > to_update_date]

        #Shift accumulated data one row backward so that we can calculate daily data later 
        to_update_vacc['fd'] = to_update_vacc.sort_values('Date').groupby('Brand')['First_Dose_Accumulate'].shift(1)
        to_update_vacc['sd'] = to_update_vacc.sort_values('Date').groupby('Brand')['Second_Dose_Accumulate'].shift(1)
        to_update_brands = list(to_update_vacc[to_update_vacc['fd'].isna()]['Brand'].unique())
        
        #To populate accumulate data for the first row in our subset of to_update table
        for items in mysql_accu_dict:
            if items['Brand'] in to_update_brands:
                to_update_vacc['fd'][to_update_vacc['Brand'] == items['Brand']] = to_update_vacc['fd'][to_update_vacc['Brand'] == items['Brand']].fillna(items['First_Dose_Accumulate'])
                to_update_vacc['sd'][to_update_vacc['Brand'] == items['Brand']] = to_update_vacc['sd'][to_update_vacc['Brand'] == items['Brand']].fillna(items['Second_Dose_Accumulate'])
                to_update_brands.remove(items['Brand'])

        #If there is a new added vaccine, we put 0 as the accumulated data for the first day
        if len(to_update_brands) != 0:
            for brand in to_update_brands:
                to_update_vacc['fd'][to_update_vacc['Brand'] == brand] = to_update_vacc['fd'][to_update_vacc['Brand'] == brand].fillna(0)
                to_update_vacc['sd'][to_update_vacc['Brand'] == brand] = to_update_vacc['sd'][to_update_vacc['Brand'] == brand].fillna(0)

        #Subtract accumulated data with accumulated data one day before to get daily data on that day
        to_update_vacc['First_Dose_Daily'] = to_update_vacc['First_Dose_Accumulate'] - to_update_vacc['fd']
        to_update_vacc['Second_Dose_Daily'] = to_update_vacc['Second_Dose_Accumulate'] - to_update_vacc['sd']
        to_update_vacc['Total_Vaccinated_Daily'] = to_update_vacc['First_Dose_Daily'] + to_update_vacc['Second_Dose_Daily']
        to_update_vacc = to_update_vacc.drop(labels=['fd','sd'],axis=1)

        return to_update_vacc

    
