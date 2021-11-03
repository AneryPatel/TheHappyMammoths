import psycopg2
import numpy as np
import pandas as pd
import type_correction as tc
import reconcilation as rec

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

# Replace 'Redacted' values to None
df_trr_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_weapondischarge_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trr.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trrstatus_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)

# List with columns by type
trr_boolean_tables = ['officer_on_duty','officer_injured','officer_in_uniform', 'subject_armed','subject_injured', 'subject_alleged_injury',
                      'notify_oemc','notify_district_sergeant', 'notify_op_command','notify_det_division']
trr_boolean_weapon_tables =['firearm_reloaded','sight_used']
trr_integer_tables =['officer_age', 'beat', 'subject_birth_year', 'subject_age', 'officer_birth_year']

"************** TYPE CORRECTION **************"
#Convert booleans
for table in trr_boolean_tables:
    tc.convert_boolean(df_trr_refresh,table)

for table in trr_boolean_weapon_tables:
    tc.convert_boolean(df_trr_weapondischarge_refresh,table)

#Convert integers
for table in trr_integer_tables:
    tc.convert_integer(df_trr_refresh,table)

#Convert timestamp (STILL STRING OUTPUT)
timestamp_created = tc.convert_timestamp(df_trr_refresh,'trr_created')
timestamp_datetime = tc.convert_timestamp(df_trr_refresh,'trr_datetime')
timestamp_status = tc.convert_timestamp(df_trr_trrstatus_refresh,'status_datetime')

#Convert dates
date_trr_app_date = tc.convert_date(df_trr_refresh,'officer_appointed_date')
date_trr_status_app_date = tc.convert_date(df_trr_trrstatus_refresh,'officer_appointed_date')


"************** RECONCILIATION **************"

# Reconciliation race
rec.reconcile_subject_race(df_trr_refresh,'subject_race')
rec.reconcile_officer_race(df_trr_refresh,'officer_race')
rec.reconcile_officer_race(df_trr_trrstatus_refresh,'officer_race')

# Reconciliation gender
rec.reconcile_gender(df_trr_refresh,'subject_gender')
rec.reconcile_gender(df_trr_refresh,'officer_gender')
rec.reconcile_gender(df_trr_trrstatus_refresh,'officer_gender')

# Reconciliation birth_year
rec.reconcile_birth_year(df_trr_refresh,'subject_birth_year')
rec.reconcile_birth_year(df_trr_refresh,'officer_birth_year')
rec.reconcile_birth_year(df_trr_trrstatus_refresh,'officer_birth_year')

# Reconciliation first name
rec.reconcile_first_name(df_trr_refresh,'officer_first_name')
rec.reconcile_first_name(df_trr_trrstatus_refresh,'officer_first_name')

# Reconciliation last name
rec.reconcile_last_name(df_trr_refresh,'officer_last_name')
rec.reconcile_last_name(df_trr_trrstatus_refresh,'officer_last_name')

# Reconciliation streets
rec.reconcile_street(df_trr_refresh,'street')

# Reconciliation locations

# Reconciliation indoor_or_outdoor

# Reconciliation party_fired_first

# Reconciliation subject weapon


# Data Integration
#print(df_trr_refresh['officer_birth_year'])
#print(df_data_officer['birth_year'])

#print(df_trr_refresh.dtypes)
#print(df_data_officer.dtypes)
left = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date']
right = ['first_name','last_name', 'gender', 'race', 'appointed_date']
# print(df_trr_refresh.shape)
merged_df = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left, right_on = right)
print(merged_df.shape)
#print(merged_df.columns)
# print(merged_df['officer_last_name'])
# print(merged_df['last_name'])

#print(merged_df['id_y'].isna().sum())
# print(merged_df['id_y'])

# Cleaning: first name, last name, gender, race, appointed_date, unit
merged_df = merged_df.drop_duplicates(['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date'], keep='last')
trr_drop = df_trr_trr.drop_duplicates(['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date'], keep='last')

print(merged_df.shape)
print(df_data_officer.shape)
print(df_trr_refresh.shape)
print(trr_drop.shape, "This is trr")
merged_df.to_csv('Integration_result.csv', header=True, index= False, sep=',')

conn.close()