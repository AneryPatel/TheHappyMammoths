import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd

df1 = pd.DataFrame({'officer_first_name':range(5),'profit':range(1000,2000,200)})
df2 = pd.DataFrame({'officer_last_name':range(2,7,1), 'stock':   ['APL','MST','JNJ','TSL','BAB']})
df3 = pd.DataFrame({'officer_birth_year':range(2,7,1), 'stock':   ['APL','MST','JNJ','TSL','BAB']})
df

display(df1)
display(df2)
