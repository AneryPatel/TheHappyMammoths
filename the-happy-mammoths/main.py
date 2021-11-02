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

#Convert booleans
type_correction2.convert_boolean(df_trr_refresh,'subject_injured')

#Convert integers
type_correction2.convert_integer(df_trr_refresh,'officer_birth_year')

#Convert timestamp
type_correction2.convert_timestamp(df_trr_refresh,'trr_created')

#Convert dates
type_correction2.convert_date(df_trr_refresh,'officer_appointed_date')



#type_correction.type_correction()



conn.close()