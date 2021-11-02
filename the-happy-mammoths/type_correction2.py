import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import datetime

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

def convert_boolean(dataframe, table):
    for i, row in dataframe.iterrows():
        value = row[table]
        if value == 'Yes':
            dataframe.at[i,table] = True
        elif value == 'Y':
            dataframe.at[i,table] = True
        elif value == 'No':
            dataframe.at[i, table] = False
        elif value == 'N':
            dataframe.at[i, table] = False

    return(dataframe[table])

def convert_integer(dataframe, table):
    for i, row in dataframe.iterrows():
        value = row[table]
        if value != None:
            dataframe.at[i, table] = int(value)
        else:
            dataframe.at[i, table] = None
    return(dataframe[table])

def convert_timestamp(dataframe, table):
    dataframe[table] = pd.to_datetime(dataframe[table], utc=True)
    dataframe[table] = dataframe[table].dt.strftime('%Y-%m-%d %I:%M:%S %p')

    return(dataframe[table])

def convert_date(dataframe, table):

    for i, row in dataframe.iterrows():
        value = row[table]
        if len(value) > 8:
            dataframe.at[i,table] = pd.to_datetime(value, format='%Y-%b-%d', errors='coerce')
        else:
            dataframe.at[i,table] = pd.to_datetime(value, format='%m-%d-%y', errors='coerce')

    for i, row in dataframe.iterrows():
        value = row[table]
        if type(value) == pd._libs.tslibs.timestamps.Timestamp:
            dataframe.at[i, table] = value.strftime('%Y-%m-%d')

    return(dataframe[table])
