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
        column_dict = {'id': 'id', 'a01': 'Country', 'a02': 'Date',\
                      'a03':'Brand', 'a04':'First_Dose_Accumulate', \
                      'a05':'Second_Dose_Accumulate', 'a06':'First_Booster_Accumulate', \
                      'a07':'First_Additional_Accumulate', 'a08':'Second_Booster_Accumulate', \
                      'a09':'Second_Additional_Accumulate', 'a10':'Total_Vaccination'}
        brand_dict = {'Oxford\/AstraZeneca': 'AstraZeneca', '高端': 'Medigen', 'BNT': 'BNT', 'Moderna':'Moderna', 'Novavax':'Novavax'}

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
        vacc_table['First_Booster_Accumulate'] = vacc_table['First_Booster_Accumulate'].astype(int)
        vacc_table['Second_Booster_Accumulate'] = vacc_table['Second_Booster_Accumulate'].astype(int)
        vacc_table['First_Additional_Accumulate'] = vacc_table['First_Additional_Accumulate'].astype(int)
        vacc_table['Second_Additional_Accumulate'] = vacc_table['Second_Additional_Accumulate'].astype(int)
        vacc_table['Total_Vaccination'] = vacc_table['Total_Vaccination'].astype(int)

        if type == 'daily':
            #Calculate daily vaccination counts
            vacc_table['fd'] = vacc_table.sort_values('Date').groupby('Brand')['First_Dose_Accumulate'].shift(1).fillna(0)
            vacc_table['sd'] = vacc_table.sort_values('Date').groupby('Brand')['Second_Dose_Accumulate'].shift(1).fillna(0)
            vacc_table['fbd'] = vacc_table.sort_values('Date').groupby('Brand')['First_Booster_Accumulate'].shift(1).fillna(0)
            vacc_table['sbd'] = vacc_table.sort_values('Date').groupby('Brand')['Second_Booster_Accumulate'].shift(1).fillna(0)
            vacc_table['fad'] = vacc_table.sort_values('Date').groupby('Brand')['First_Additional_Accumulate'].shift(1).fillna(0)
            vacc_table['sad'] = vacc_table.sort_values('Date').groupby('Brand')['Second_Additional_Accumulate'].shift(1).fillna(0)
            vacc_table['First_Dose_Daily'] = vacc_table['First_Dose_Accumulate'] - vacc_table['fd']
            vacc_table['Second_Dose_Daily'] = vacc_table['Second_Dose_Accumulate'] - vacc_table['sd']
            vacc_table['First_Booster_Daily'] = vacc_table['First_Booster_Accumulate'] - vacc_table['fbd']
            vacc_table['Second_Booster_Daily'] = vacc_table['Second_Booster_Accumulate'] - vacc_table['sbd']
            vacc_table['First_Additional_Daily'] = vacc_table['First_Additional_Accumulate'] - vacc_table['fad']
            vacc_table['Second_Additional_Daily'] = vacc_table['Second_Additional_Accumulate'] - vacc_table['sad']            
            vacc_table['Third_Dose_Beyond_Daily'] = vacc_table['First_Booster_Daily'] + vacc_table['Second_Booster_Daily'] +vacc_table['First_Additional_Daily'] + vacc_table['Second_Additional_Daily']       
            vacc_table['Total_Vaccinated_Daily'] = vacc_table['First_Dose_Daily'] + vacc_table['Second_Dose_Daily'] + vacc_table['Third_Dose_Beyond_Daily']          
            vacc_table = vacc_table.drop(labels=['fd','sd','fbd','sbd','fad','sad'],axis=1)
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
                     sum(Second_Dose_Daily) AS Second_Dose_Accumulate,
                     sum(Third_Dose_Beyond_Daily) AS Third_Dose_Beyond_Accumulate
                     FROM covid19_vaccination 
                     WHERE Date <= '{}'
                     GROUP BY (Brand)""".format(to_update_date_str)
            #Run the SQL command and fetch all results
            cursor.execute(sql)
            result=cursor.fetchall()
        
        return result
