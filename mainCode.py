from io import StringIO #extract
import requests #extract
from bs4 import BeautifulSoup #extract
import pandas as pd # transformation
import sqlite3 #load
from datetime import datetime

print('hello')
def log_progress(message):
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""

    with open('./logs/code_log.txt', 'a') as f:
        f.write(f'{datetime.now()}: {message}\n')

# Step 1
def extract(url, table_att):
    soup = requests.get(url).text
    web = BeautifulSoup(soup, 'html.parser')
    table = web.find('span', string=table_att).find_next('table')
    df = pd.read_html(StringIO(str(table)))[0]

    log_progress('Data Extracted Successfully from the Wikipedia')

    return df

# Step 2

def transformation(df, csv_path):
    exchange_rate = pd.read_csv(csv_path, index_col = 0).to_dict()['Rate']

    df['MC_GBP_Billion'] = round(df['Market cap (US$ billion)']*exchange_rate['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['Market cap (US$ billion)']*exchange_rate['EUR'], 2)
    df['MC_INR_Billion'] = round(df['Market cap (US$ billion)']*exchange_rate['INR'], 2)

   # print(df['MC_EUR_Billion'][4])

    log_progress('Data Transformation done now, Step 2 completed.')

    return df
# Step 3 (Load)
#CSV

def load_to_csv(df, out_path):
    df.to_csv(out_path)

    log_progress('Data loaded into the CSV File')

#SQLite

def load_to_db(df, sql_conn, tn):
    df.to_sql(tn, sql_conn, if_exists='replace', index=False)

    log_progress('Data loaded into the SQLite Database')

if __name__ =='__main__':
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    output_csv_path = './output/Largest_banks_data.csv'
    database_name = './output/Banks.db'
    table_name = 'Largest_banks'
        
    log_progress('Preliminaries complete. Initiating ETL process')
        
    df = (extract(url, 'By market capitalization'))
    log_progress('Extraction Completed')
    print(df)

    load_to_csv(df, output_csv_path)
    log_progress('Dataframe has been iploaded into the CSV file on our PC!')


    df = transformation(df, './input/exchange_rate.csv')
    log_progress('Data Transformed Successfully')
    print(df)

    with sqlite3.connect(database_name) as conn:
        load_to_db(df, conn, table_name)
    log_progress('DataFrame has been uploaded into the SQLite Database Successfully!')