import psycopg2 as psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

cur = conn.cursor()
cur.execute("SELECT * FROM trr_trr_refresh")

df_trr_trr_refresh = pd.read_sql_query('select * from trr_trr_refresh', con=conn)
df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)

file_name= "df_trr_trr_refresh.csv"
file_name1= 'C:/Users/anita/Desktop/MSAI/Fall 21/Data Science/trr_trr_refresh1.csv'
df_trr_trr_refresh.to_csv(file_name1, sep='\t')

print(df_trr_trr_refresh)

conn.close()