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

# Reconciliation officer_race
def reconcile_officer_race(dataframe, table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    dataframe[table].replace(to_replace='Amer Ind/Alaskan Native', value='Native American/Alaskan Native', inplace=True)
    dataframe[table].replace(to_replace='Black Hispanic', value='Black', inplace=True)
    dataframe[table].replace(to_replace='White Hispanic', value='White', inplace=True)
    dataframe[table].replace(to_replace='Asian/Pacific Islander', value='Asian/Pacific', inplace=True)
    return(dataframe[table])

# Reconciliation gender
def reconcile_gender(dataframe, table):
    dataframe[table].replace(to_replace='FEMALE', value='F', inplace=True)
    dataframe[table].replace(to_replace='MALE', value='M', inplace=True)
    return (dataframe[table])

# Reconciliation birth_year (subject, officer and status)
def reconcile_birth_year(dataframe,table):
    for i, row in dataframe.iterrows():
        value = row[table]
        if value != None:
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



reconcile_first_name(df_trr_trrstatus_refresh,'officer_first_name')

set_original = set(df_data_officer['first_name'])
set_difference = set(df_trr_trrstatus_refresh['officer_first_name'])-set(df_data_officer['first_name'])

print(set_difference)
#print(set_original)

