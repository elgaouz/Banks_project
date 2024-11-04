# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

log_file = "log_file.txt"
url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
output_path="./Largest_banks_data.csv"
table_name="Largest_banks"



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format='%y-%m-%D-%h:%m:%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(log_file,'a')as f:
        f.write(timestamp +':' +message +'\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page=requests.get(url).text
    data=BeautifulSoup(html_page,'html.parser')
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    df=pd.DataFrame(columns=['Name','MC_USD_Billion'])
    
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            data_dict={'Name': col[1].find('a',recursive=False)['title'] ,
                        'MC_USD_Billion':float(col[2].contents[0])
            }
            df1=pd.DataFrame(data_dict, index=[0])
            df=pd.concat([df,df1], ignore_index=True)            
   
    return df
table_attribs="wikitable sortable mw-collapsible jquery-tablesorter mw-made-collapsible" #regarde html la balise <table>
#print(extract(url,table_attribs))

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies''' 

    df['MC_GBP_Billion'] = [np.round(x*0.8,2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*0.93,2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*82.95,2) for x in df['MC_USD_Billion']]

    return df
df=transform(extract(url,table_attribs),csv_path)
#print(df['MC_EUR_Billion'][4])

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

#load_to_csv(df,output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace', index=False)

#load_to_db(df,sql_connection,table_name)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement,sql_connection))



''' Here, we define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress('Preliminaries complete. Initiating ETL process')
extracted_data=extract(url,table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
transformed_data=transform(extracted_data,csv_path)

log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(transformed_data,output_path)
log_progress('Data saved to CSV file')

sql_connection=sqlite3.connect("Banks.db")
log_progress('SQL Connection initiated')

load_to_db(transformed_data,sql_connection,table_name)
log_progress('Data loaded to Database as a table, Executing queries')

print("Print the contents of the entire table")
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement,sql_connection)

print("Print the average market capitalization of all the banks in Billion USD.")
query_statement=f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement,sql_connection)

print("Print only the names of the top 5 banks")
query_statement=f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement,sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')