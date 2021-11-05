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

