import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
# import type_correction2

conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
df_trr = pd.read_sql_query("select * from trr_trr", con=conn)

# ---- Party_fired_first -----

# print(df_trr_refresh['party_fired_first'].unique())
# print(df_trr['party_fired_first'].unique())

# --- Street ---

df_trr_refresh['street'] = df_trr_refresh['street'].str.lower()
df_trr_refresh['street'] = df_trr_refresh['street'].str.title()
# print(set(df_trr_refresh['street'].unique())-set(df_trr['street'].unique()))


# --- Weapon Type ----
df_trr_subjectweapon = pd.read_sql_query('select * from trr_subjectweapon', con=conn)
df_trr_subjectweapon_refresh = pd.read_sql_query('select * from trr_subjectweapon_refresh', con=conn)

# print(set(df_trr_subjectweapon['weapon_type'].unique())- set(df_trr_subjectweapon_refresh['weapon_type'].unique()))
# print(df_trr_subjectweapon_refresh['weapon_type'].unique())


# ---- Indoor or Outdoor -----

df_trr_refresh.indoor_or_outdoor.replace(to_replace = 'OUTDOOR', value = 'Outdoor', inplace = True)
df_trr_refresh.indoor_or_outdoor.replace(to_replace = 'INDOOR', value = 'Indoor', inplace = True)

# --- Location ----

df_trr_refresh['location'] = df_trr_refresh['location'].str.lower()
df_trr_refresh['location'] = df_trr_refresh['location'].str.title()
df_trr_refresh['location'] = df_trr_refresh['location'].str.replace(' / ', '/')

# 'Airport Terminal Mezzanine - Non-Secure Area', 'Airport Parking Lot'}

df_trr_refresh['location'].replace(to_replace = 'College/University Grounds', value = 'College/University - Grounds', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Other (Specify)', value = 'Other', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'School - Public Grounds', value = 'School, Public, Grounds', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'School - Private Grounds', value = 'School, Private, Grounds', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'School - Private Building', value = 'School, Private, Building', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'School - Public Building', value = 'School, Public, Building', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Vehicle - Other Ride Share Service (Lyft, Uber, Etc.)', value = 'Vehicle - Other Ride Service', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Residence - Porch/Hallway', value = 'Residence Porch/Hallway', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Residence - Yard (Front/Back)', value = 'Residential Yard (Front/Back)', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Residence - Garage', value = 'Residence-Garage', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Cta Parking Lot/Garage/Other Property', value = 'Cta Garage / Other Property', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Jail/Lock-Up Facility', value = 'Jail / Lock-Up Facility', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Nursing/Retirement Home', value = 'Nursing Home/Retirement Home', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Parking Lot/Garage (Non Residential)', value = 'Parking Lot/Garage(Non.Resid.)', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Police Facility/Vehicle Parking Lot', value = 'Police Facility/Veh Parking Lot', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Vehicle - Commercial', value = 'Vehicle-Commercial', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Other Railroad Property/Train Depot', value = 'Other Railroad Prop / Train Depot', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Other Railroad Prop/Train Depot', value = 'Other Railroad Prop / Train Depot', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'College/University - Grounds', value = 'College/University Grounds', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Cta Garage/Other Property', value = 'Cta Garage / Other Property', inplace = True)
df_trr_refresh['location'].replace(to_replace = 'Commercial/Business Office', value = 'Commercial / Business Office', inplace = True)
#
# print(set(df_trr_refresh['location'].unique())-set(df_trr['location'].unique()))

# print(df_trr['location'].unique(),sep = "\n")