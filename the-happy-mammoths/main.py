import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import type_correction
import type_correction2

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)

# List with tables by type
trr_boolean_tables = ['officer_on_duty','officer_injured','officer_in_uniform', 'subject_armed','subject_injured', 'subject_alleged_injury',
                      'notify_oemc','notify_district_sergeant', 'notify_op_command','notify_det_division']
trr_boolean_weapon_tables =['firearm_reloaded','sight_used']
trr_integer_tables =['officer_age', 'beat', 'subject_birth_year', 'subject_age', 'officer_birth_year']


#Convert booleans
for table in trr_boolean_tables:
    type_correction2.convert_boolean(df_trr_refresh,table)

for table in trr_boolean_weapon_tables:
    type_correction2.convert_boolean(df_trr_weapondischarge_refresh,table)

#Convert integers
for table in trr_integer_tables:
    type_correction2.convert_integer(df_trr_refresh,table)

#Convert timestamp
timestamp_created = type_correction2.convert_timestamp(df_trr_refresh,'trr_created')
timestamp_datetime = type_correction2.convert_timestamp(df_trr_refresh,'trr_datetime')
timestamp_status = type_correction2.convert_timestamp(df_trr_trrstatus_refresh,'status_datetime')

#Convert dates
date_trr_app_date = type_correction2.convert_date(df_trr_refresh,'officer_appointed_date')
date_trr_status_app_date = type_correction2.convert_date(df_trr_trrstatus_refresh,'officer_appointed_date')









conn.close()