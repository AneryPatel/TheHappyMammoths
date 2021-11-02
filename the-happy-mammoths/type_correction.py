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

def type_correction():

    df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
    df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
    df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
    df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)

    #Convert Booleans
    trr_boolean_tables = ['officer_on_duty','officer_injured','officer_in_uniform', 'subject_armed','subject_injured', 'subject_alleged_injury',
                      'notify_oemc','notify_district_sergeant', 'notify_op_command','notify_det_division']
    for table in trr_boolean_tables:
        for row in df_trr_refresh[table].values:
            if row == 'Yes':
                df_trr_refresh[table] = True
            elif row == 'Y':
                df_trr_refresh[table] = True
            elif row == 'No':
                df_trr_refresh[table] = False
            elif row == 'N':
                df_trr_refresh[table] = False

    trr_boolean_weapon_tables =['firearm_reloaded','sight_used']
    for table in trr_boolean_weapon_tables:
        for row in df_trr_weapondischarge_refresh[table].values:
            if row == 'Yes':
                df_trr_refresh[table] = True
            elif row == 'Y':
                df_trr_refresh[table] = True
            elif row == 'No':
                df_trr_refresh[table] = False
            elif row == 'N':
                df_trr_refresh[table] = False


    #Convert Integers
    df_trr_refresh['officer_age'] = df_trr_refresh['officer_age'].astype('int64')
    df_trr_refresh['beat'] = df_trr_refresh['beat'].astype('int64')
    df_trr_refresh['subject_birth_year'] = df_trr_refresh['subject_birth_year'].astype('int64')
    df_trr_refresh['subject_age'] = df_trr_refresh['subject_age'].astype('int64')
    df_trr_refresh['officer_birth_year'] = pd.to_numeric(df_trr_refresh['officer_birth_year'], downcast='integer', errors='coerce')


    trr_date_tables = ['trr_created', 'trr_datetime','officer_appointed_date']
    trr_status_date_tables = ['status_datetime', 'officer_appointed_date']

    #Convert timestamp
    df_trr_refresh['trr_created'] = pd.to_datetime(df_trr_refresh['trr_created'], utc = True)
    df_trr_refresh['trr_created'] = df_trr_refresh['trr_created'].dt.strftime('%Y-%m-%d %I:%M:%S %p')

    df_trr_refresh['trr_datetime'] = pd.to_datetime(df_trr_refresh['trr_datetime'], utc = True)
    df_trr_refresh['trr_datetime'] = df_trr_refresh['trr_datetime'].dt.strftime('%Y-%m-%d %I:%M:%S %p')

    df_trr_trrstatus_refresh['status_datetime'] = pd.to_datetime(df_trr_trrstatus_refresh['status_datetime'], utc = True)
    df_trr_trrstatus_refresh['status_datetime'] = df_trr_trrstatus_refresh['status_datetime'].dt.strftime('%Y-%m-%d %I:%M:%S %p')


    #Convert dates
    for i,row in df_trr_trrstatus_refresh.iterrows():
        value = row['officer_appointed_date']

        if len(value) > 8:
            df_trr_trrstatus_refresh.at[i,'officer_appointed_date'] = pd.to_datetime(value, format='%Y-%b-%d', errors='coerce')
        else:
            df_trr_trrstatus_refresh.at[i,'officer_appointed_date'] = pd.to_datetime(value, format='%m-%d-%y', errors='coerce')

    for i,row in df_trr_refresh.iterrows():
        value = row['officer_appointed_date']
        if len(value) > 8:
            df_trr_refresh.at[i,'officer_appointed_date'] = pd.to_datetime(value, format='%Y-%b-%d', errors='coerce')
        else:
            df_trr_refresh.at[i,'officer_appointed_date'] = pd.to_datetime(value, format='%m-%d-%y', errors='coerce')

    for i, row in df_trr_refresh.iterrows():
        value = row['officer_appointed_date']
        if type(value) == pd._libs.tslibs.timestamps.Timestamp:
            df_trr_refresh.at[i, 'officer_appointed_date'] = value.strftime('%Y-%m-%d')

    for i, row in df_trr_trrstatus_refresh.iterrows():
        value = row['officer_appointed_date']
        if type(value) == pd._libs.tslibs.timestamps.Timestamp:
            df_trr_trrstatus_refresh.at[i, 'officer_appointed_date'] = value.strftime('%Y-%m-%d')
            #df_trr_trrstatus_refresh.at[i, 'officer_appointed_date'] = pd.to_datetime(value.strftime('%Y-%m-%d'), format='%Y-%m-%d')
            #print(type(df_trr_trrstatus_refresh.at[i, 'officer_appointed_date']))

    #print(df_trr_refresh.dtypes)

    print(df_trr_trrstatus_refresh['officer_appointed_date'])

