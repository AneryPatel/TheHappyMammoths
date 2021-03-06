import psycopg2
import pandas as pd
import datetime

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)

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
        # dataframe.at[i, table] = bool(dataframe.at[i,table])
    return(dataframe[table])

def convert_integer(dataframe, table):
    # dataframe[table] = dataframe[table].astype(int)
    for i, row in dataframe.iterrows():
        value = row[table]
        if value != None:
            dataframe.at[i, table] = int(value)
    return(dataframe[table])

def convert_timestamp(dataframe, table):
    dataframe[table] = pd.to_datetime(dataframe[table], utc=True)
    #dataframe[table] = dataframe[table].dt.strftime('%Y-%m-%d %I:%M:%S %p')
    #dataframe[table] = datetime.datetime(dataframe[table].year, dataframe[table].month,dataframe[table].day,
    #                                     dataframe[table].hour, dataframe[table].minute, dataframe[table].second)

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
            dataframe.at[i, table] = datetime.date(value.year,value.month, value.day)

    return(dataframe[table])