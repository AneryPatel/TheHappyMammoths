import pandas as pd
import numpy as np
import psycopg2

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)
df_data_officer = pd.read_sql_query("select * from data_officer", con=conn)


# Reconciliation subject_race
def reconcile_subject_race(dataframe, table):
    dataframe[table].replace(to_replace='ASIAN / PACIFIC ISLANDER', value='ASIAN/PACIFIC ISLANDER', inplace=True)
    dataframe[table].replace(to_replace='UNKNOWN / REFUSED', value=None, inplace=True)
    dataframe[table].replace(to_replace='UNKNOWN', value=None, inplace=True)
    dataframe[table].replace(to_replace='AMER IND/ALASKAN NATIVE', value='NATIVE AMERICAN/ALASKAN NATIVE', inplace=True)
    dataframe[table].replace(to_replace='AMER INDIAN / ALASKAN NATIVE', value='NATIVE AMERICAN/ALASKAN NATIVE', inplace=True)
    return(dataframe[table])

# Reconciliation subject_gender
def reconcile_subject_gender(dataframe, table):
    dataframe[table].replace(to_replace='FEMALE', value='F', inplace=True)
    dataframe[table].replace(to_replace='MALE', value='M', inplace=True)
    return (dataframe[table])

# Reconciliation subject_birth_year
def reconcile_birth_year(dataframe,table):
    for i, row in dataframe.iterrows():
        value = row[table]
        value = float(value)
        dataframe.at[i, table] = value
        if value < 100 and value > 9:
            dataframe.at[i,table] = 1900+value
    return(dataframe[table])

#Reconcilation first name
def reconcile_first_name(dataframe,table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    return(dataframe[table])

#Reconcilation last name
def reconcile_last_name(dataframe,table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    # Suffix
    suffix_list = ['Jr','Sr', 'I',"Ii",'Iii','Iv','V']
    return(dataframe[table])



reconcile_birth_year(df_trr_refresh,'officer_birth_year')

set_original = set(df_data_officer['birth_year'])
set_difference = set(df_trr_refresh['officer_birth_year'])-set(df_data_officer['birth_year'])

print(set_difference)

