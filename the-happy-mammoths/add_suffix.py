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

def add_suffix_column(dataframe, table):
    suffix_list = []
    for i in dataframe[table]:

        if 'III' in i[-3:]:
            suffix_list.append('III')
        elif ('IV' in i[-2:] and len(i)>4):
            suffix_list.append('IV')
        elif ('I' in i[-1:] and i[-2] ==' '):
            suffix_list.append('I')
        elif('II' in i[-2:]):
            suffix_list.append('II')
        elif ('V' in i[-2:] and i[-2] == ' '):
            suffix_list.append('V')
        elif ('SR' in i[-2:] and i[-3] ==' '):
            suffix_list.append('SR')
        elif ('JR' in i[-2:] and i[-3] ==' '):
            suffix_list.append('JR')
        else:
            suffix_list.append('')

    dataframe[table] = suffix_list

    return(dataframe[table])

