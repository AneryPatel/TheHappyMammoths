import pandas as pd
import psycopg2
import type_correction as tc

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


# Reconciliation subject_race
def reconcile_subject_race(dataframe, table):
    dataframe[table].replace(to_replace='ASIAN / PACIFIC ISLANDER', value='ASIAN/PACIFIC ISLANDER', inplace=True)
    dataframe[table].replace(to_replace='UNKNOWN / REFUSED', value=None, inplace=True)
    dataframe[table].replace(to_replace='UNKNOWN', value=None, inplace=True)
    dataframe[table].replace(to_replace='AMER IND/ALASKAN NATIVE', value='NATIVE AMERICAN/ALASKAN NATIVE', inplace=True)
    dataframe[table].replace(to_replace='AMER INDIAN / ALASKAN NATIVE', value='NATIVE AMERICAN/ALASKAN NATIVE', inplace=True)
    return(dataframe[table])

# Reconciliation officer_race
def reconcile_officer_race(dataframe, table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    dataframe[table].replace(to_replace='Amer Ind/Alaskan Native', value='Native American/Alaskan Native', inplace=True)
    dataframe[table].replace(to_replace='Black Hispanic', value='Hispanic', inplace=True)
    dataframe[table].replace(to_replace='White Hispanic', value='Hispanic', inplace=True)
    dataframe[table].replace(to_replace='Asian/Pacific Islander', value='Asian/Pacific', inplace=True)
    return(dataframe[table])

# Reconciliation gender
def reconcile_gender(dataframe, table):
    dataframe[table].replace(to_replace='FEMALE', value='F', inplace=True)
    dataframe[table].replace(to_replace='MALE', value='M', inplace=True)
    return (dataframe[table])

# Reconciliation birth_year (subject, officer and status)
def reconcile_birth_year(dataframe,table):
    for i, row in dataframe.iterrows():
        value = row[table]
        if value != None:
            value = int(value)
            dataframe.at[i, table] = value
            if value < 100 and value > 10:
                dataframe.at[i,table] = 1900 + value
            if value <= 10:
                dataframe.at[i, table] = 2000 + value
        else:
            dataframe.at[i, table] = int(0)
    return(dataframe[table])

# Reconciliation first name
def reconcile_first_name(dataframe,table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    return(dataframe[table])

# Reconciliation last name
def reconcile_last_name(dataframe,table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    dataframe[table] = dataframe[table].str.replace(' Jr', '',regex=True)
    dataframe[table] = dataframe[table].str.replace(' Jr.', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' Sr', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' I', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' Ii', '', regex=True)
    dataframe[table] = dataframe[table].str.replace('ii', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' Iii', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' Iv', '', regex=True)
    dataframe[table] = dataframe[table].str.replace(' V', '', regex=True)
    dataframe[table] = dataframe[table].str.replace('. ', '', regex=True)
    dataframe[table] = dataframe[table].str.replace('.', '', regex=True)
    return(dataframe[table])


# Reconciliation locations
def reconcile_location(dataframe, table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    dataframe[table] = dataframe[table].str.replace(' / ', '/')

    # 'Airport Terminal Mezzanine - Non-Secure Area', 'Airport Parking Lot' added

    dataframe[table].replace(to_replace='College/University Grounds', value='College/University - Grounds',inplace=True)
    dataframe[table].replace(to_replace='Other (Specify)', value='Other', inplace=True)
    dataframe[table].replace(to_replace='School - Public Grounds', value='School, Public, Grounds',inplace=True)
    dataframe[table].replace(to_replace='School - Private Grounds', value='School, Private, Grounds',inplace=True)
    dataframe[table].replace(to_replace='School - Private Building', value='School, Private, Building',inplace=True)
    dataframe[table].replace(to_replace='School - Public Building', value='School, Public, Building',inplace=True)
    dataframe[table].replace(to_replace='Vehicle - Other Ride Share Service (Lyft, Uber, Etc.)',value='Vehicle - Other Ride Service', inplace=True)
    dataframe[table].replace(to_replace='Residence - Porch/Hallway', value='Residence Porch/Hallway',inplace=True)
    dataframe[table].replace(to_replace='Residence - Yard (Front/Back)',value='Residential Yard (Front/Back)', inplace=True)
    dataframe[table].replace(to_replace='Residence - Garage', value='Residence-Garage', inplace=True)
    dataframe[table].replace(to_replace='Cta Parking Lot/Garage/Other Property',value='Cta Garage / Other Property', inplace=True)
    dataframe[table].replace(to_replace='Jail/Lock-Up Facility', value='Jail / Lock-Up Facility',inplace=True)
    dataframe[table].replace(to_replace='Nursing/Retirement Home', value='Nursing Home/Retirement Home',inplace=True)
    dataframe[table].replace(to_replace='Parking Lot/Garage (Non Residential)',value='Parking Lot/Garage(Non.Resid.)', inplace=True)
    dataframe[table].replace(to_replace='Police Facility/Vehicle Parking Lot',value='Police Facility/Veh Parking Lot', inplace=True)
    dataframe[table].replace(to_replace='Vehicle - Commercial', value='Vehicle-Commercial', inplace=True)
    dataframe[table].replace(to_replace='Other Railroad Property/Train Depot',value='Other Railroad Prop / Train Depot', inplace=True)
    dataframe[table].replace(to_replace='Other Railroad Prop/Train Depot',value='Other Railroad Prop / Train Depot', inplace=True)
    dataframe[table].replace(to_replace='College/University - Grounds', value='College/University Grounds',inplace=True)
    dataframe[table].replace(to_replace='Cta Garage/Other Property', value='Cta Garage / Other Property',inplace=True)
    dataframe[table].replace(to_replace='Commercial/Business Office', value='Commercial / Business Office',inplace=True)
    return dataframe[table]

# Reconciliation streets
def reconcile_street(dataframe,table):
    dataframe[table] = dataframe[table].str.lower()
    dataframe[table] = dataframe[table].str.title()
    dataframe[table].replace(to_replace='126Th Pl', value='126Th St', inplace=True)
    dataframe[table].replace(to_replace='17Th Ave.', value='17Th St', inplace=True)
    dataframe[table].replace(to_replace='49Th Av', value='49Th St', inplace=True)
    dataframe[table].replace(to_replace='95Th Pl', value='95Th St', inplace=True)
    dataframe[table].replace(to_replace='Albany', value='Albany Ave', inplace=True)
    dataframe[table].replace(to_replace='B2 St', value='B20 St', inplace=True)
    dataframe[table].replace(to_replace='Beach', value='Beach Ave', inplace=True)
    dataframe[table].replace(to_replace='Beach St', value='Beach Ave', inplace=True)
    dataframe[table].replace(to_replace='Beech St', value='Beach Ave', inplace=True)
    dataframe[table].replace(to_replace='Belmont Harbor Dr', value='Belmont Ave', inplace=True)
    dataframe[table].replace(to_replace='Bloomingdale Trl', value='Bloomingdale Ave', inplace=True)
    dataframe[table].replace(to_replace='Broadway St', value='Broadway', inplace=True)
    dataframe[table].replace(to_replace='Carver Plz', value='Carver Dr', inplace=True)
    dataframe[table].replace(to_replace='Cleveland St', value='Cleveland Ave', inplace=True)
    dataframe[table].replace(to_replace='Columbia Dr', value='Columbia Ave', inplace=True)
    dataframe[table].replace(to_replace='Foster Pl', value='Foster Dr', inplace=True)
    dataframe[table].replace(to_replace='Green', value='Green St', inplace=True)
    dataframe[table].replace(to_replace='Gregory', value='Ggregory', inplace=True)
    dataframe[table].replace(to_replace='Harper Ct', value='Harper Ave', inplace=True)
    dataframe[table].replace(to_replace='Lower Wacker Pl', value='Lower Wacker Dr', inplace=True)
    dataframe[table].replace(to_replace='Luis Munoz Marin Dr E', value='Luis Munoz Marin Dr W', inplace=True)
    dataframe[table].replace(to_replace='Luis Munoz Marin Dr N', value='Luis Munoz Marin Dr W', inplace=True)
    dataframe[table].replace(to_replace='Midway St', value='Midway Park', inplace=True)
    dataframe[table].replace(to_replace='Milwaukee Av', value='Milwaukee Ave', inplace=True)
    dataframe[table].replace(to_replace='Orland Square Drive', value='Orland Square Dr', inplace=True)
    dataframe[table].replace(to_replace='Park Ridge Rd', value='Ridge Ave', inplace=True)
    dataframe[table].replace(to_replace='Roosevelt', value='Roosevelt Dr', inplace=True)
    return (dataframe[table])

def reconcile_in_outdoor(dataframe, table):
    dataframe[table].replace(to_replace = 'OUTDOOR', value = 'Outdoor', inplace = True)
    dataframe[table].replace(to_replace = 'INDOOR', value = 'Indoor', inplace = True)
    return (dataframe[table])


