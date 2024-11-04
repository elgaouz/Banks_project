import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

url="https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
db_name='World_Economies.db'
table_name='Countries_by_GDP'
json_path='Countries_by_GDP.json'
df=pd.DataFrame(columns=['Country', 'GDP_USD_billion'])

html_page=requests.get(url).text
data=BeautifulSoup(html_page, 'html.parser')

tables=data.find_all('tbody')
rows=tables[0].find_all('tr')

def transform(data):
    if data.Estimate!=None: 
        data['GDP_USD_billion']=round(data.Estimate*(1/1000),2) 
    else: data['GDP_USD_billion']=None
    return data

for row in rows:
    col=row.find_all('td')
    if len(col)!=0:
        data_dict={ "Country": str(col[0].contents[0]),
                    "GDP_USD_billion": transform((col[0])) }
        df1=pd.DataFrame(data_dict)
        df=pd.concat([df,df1], ignore_index=True)

print(df)

df.to_json(json_path)

conn=sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()

