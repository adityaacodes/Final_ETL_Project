import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import configparser
import os


def connect_to_db(password):
    con = psycopg2.connect(database='Banks',
                           user='postgres',
                           password=password,
                           host='127.0.0.1',
                           port='5432')

    engine = create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/Banks", pool_pre_ping=True)

    return con, engine


def extract(url, table_attribs):
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'rank': col[0].text.replace('\n', ''),
                         'name': col[1].text.replace('\n', ''),
                         'mc_usd_billion': col[2].text.replace('\n', '')}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, exchange_rate):
    df['mc_eur_billion'] = [np.round(float(x) * exchange_rate['Rate'][0], 2) for x in df['mc_usd_billion']]
    df['mc_gbp_billion'] = [np.round(float(x) * exchange_rate['Rate'][1], 2) for x in df['mc_usd_billion']]
    df['mc_inr_billion'] = [np.round(float(x) * exchange_rate['Rate'][2], 2) for x in df['mc_usd_billion']]
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(df, engine, table_name):
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def run_query(statement, engine):
    return pd.read_sql(statement, engine)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')


url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['rank', 'name', 'mc_usd_billion']
exchange_rate = pd.read_csv('exchange_rate.csv')
csv_path = os.path.join(os.getcwd(), "transformed_data.csv")
table_name = 'largest_banks'
log_file = 'log_file.txt'


# Getting the PostgreSQL password from the config.ini file
config = configparser.ConfigParser()
config.read('config.ini')
password = config['secret']['PASSWORD']


# Log the initialization of the ETL process
log_progress('Preliminaries complete. Initiating ETL process')

# Log the beginning of the Extraction process
df = extract(url, table_attribs)

# Log the completion of the Extraction process and the beginning of the Transformation process
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, exchange_rate)

# Log the completion of the Transformation process and the beginning of the Loading process
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
con, engine = connect_to_db(password)
log_progress('SQL Connection initiated.')
load_to_db(df, engine, table_name)

# Log the completion of the Loading process and run an Example query
log_progress('Data loaded to Database as table. Running the query')
query_statement = f'SELECT * FROM {table_name}'
print(run_query(query_statement, engine))
query_statement = f'SELECT AVG(mc_gbp_billion) FROM {table_name}'
print(run_query(query_statement, engine))
query_statement = f'SELECT name from {table_name} LIMIT 5'
print(run_query(query_statement, engine))

# Log the completion of the ETL process
log_progress('Process Complete.')

con.close()
