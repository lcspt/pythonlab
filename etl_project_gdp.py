# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # skip "WORLD" row, not a country
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')[2:]
    for row in rows:
        col = row.find_all('td')
        if col[2] > 10000000:
            print(col[0])
        else:
            break    

    return df

def transform(df):
	''' This function converts the GDP information from Currency
	format to float value, transforms the information of GDP from
	USD (Millions) to USD (Billions) rounding to 2 decimal places.
	The function returns the transformed dataframe.'''

	return df

def load_to_csv(df, csv_path):
	''' This function saves the final dataframe as a `CSV` file 
	in the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
	''' This function saves the final dataframe as a database table
	with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
	''' This function runs the stated query on the database table and
	prints the output on the terminal. Function returns nothing. '''

def log_progress(message):
	''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
attributes = ['Country', 'GDP_USD_billion']
extract(url, attributes)