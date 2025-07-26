# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import requests as rq

# Defining the required variables
data_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
exchange_rate_path = "exchange_rate.csv"
table_name = 'Largest_banks'
output_path = "./Largest_banks_data.csv"
db_name = 'Banks.db'
log_file = 'code_log.txt'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stap_fmt = "%d/%m/%Y %I:%M:%S%p"
    time_stap = datetime.now()
    time_stap = time_stap.strftime(time_stap_fmt)
    with open(log_file, 'a') as file:
        file.write(f"{time_stap} - {message}\n")

def extract(url, attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = rq.get(url, timeout=10).text
    soup = BeautifulSoup(response, 'html.parser')
    table=soup.tbody
    rows = table.find_all('tr')
    data_list = []
    for row in rows:
        cols = row.find_all('td')
        # Rank, Bank name, Market cap (US$ billion)
        if len(cols) == 3:
            # rank = cols[0].text.strip()
            name = cols[1].text.strip()
            market_cap = cols[2].text.strip()
            # Append the data to the list
            data_list.append({"Name": name, "MC_USD_Billion": float(market_cap)})

    return pd.DataFrame(data_list, columns=attribs)


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    with open(csv_path, 'r') as file:
        exchange_rate = pd.read_csv(file, index_col=0)
    for currency, rate in exchange_rate.items():
        df[f'MC_{currency}_Billion'] = df['MC_USD_Billion'] * rate

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")

df=extract(data_url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, exchange_rate_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query = f"SELECT * FROM {table_name}"
run_query(query, conn)
log_progress("Process Complete")

conn.close()
log_progress("Server Connection closed")
