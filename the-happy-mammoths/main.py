import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import type_correction

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

type_correction.type_correction()



conn.close()