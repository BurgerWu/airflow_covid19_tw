import requests
import re 
import pandas as pd
class LoadTableFunctions:
    def get_vaccination_table(url, type = 'daily'):
        """
        This function retrieves vaccination information from National Center for High-Performance Computing (NCHC) 
        along with the help of text manipulation to get original table.
    
        Input: Target URL
        Output: Latest vaccination table
        """
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
            pass

        #Return target vaccination table
        return vacc_table


    
