import pandas as pd
import numpy as np
import psycopg2
import type_correction as tc

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
# df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
# df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
# df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)
df_data_officer = pd.read_sql_query("select * from data_officer", con=conn)

# print(df_trr_refresh.columns,"Index trr refresh")
# print(df_data_officer.columns,"Index data officer")

'''
Match: Trr_refresh(officer_first_name) = data_officer(first_name) **
    Trr_refresh(officer_last_name) = data_officer(last_name) **
    Trr_refresh(officer_middle_initial) = data_officer(middle_initial) 
    Trr_refresh(officer_gender) = data_officer(gender) **
    Trr_refresh(officer_race) = data_officer(race) **
    Trr_refresh(officer_birth_year) = data_officer(birth_year) **
    Trr_refresh(officer_appointed_date) = data_officer(appointed_date) **
    Trr_refresh(create suffix) = data_officer(suffix_name) (later) 
    Trr_refresh(officer_unit_name) = data_officer(merge with last_unit_id to data_policeunit) (later)
    
    If match = True -> data_officer(id) -> store in Trr_refresh(officer_id)
'''

left = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race', 'officer_birth_year','officer_appointed_date']
right = ['first_name','last_name', 'gender', 'race', 'birth_year', 'appointed_date']
print(df_trr_refresh.shape)
merged_df = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left, right_on = right)
print(merged_df.shape)



'''
    for i, row in df_trr_refresh.iterrows():
        value = row[table]
        if value == 'Yes':
'''