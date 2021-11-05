import psycopg2
import numpy as np
import pandas as pd
import type_correction as tc
import reconcilation as rec
import add_suffix as add_suffix

print("***** Connecting to CDDPB Postgres database  *****")

# ----- Connect to the PostgreSQL Database -------
conn = psycopg2.connect(
    host="codd01.research.northwestern.edu",
    database="postgres",
    user="cpdbstudent",
    password="DataSci4AI")

# ------ Import tables into dataframes --------
df_trr_refresh = pd.read_sql_query("select * from trr_trr_refresh", con=conn)
df_trr_weapondischarge_refresh = pd.read_sql_query('select * from trr_weapondischarge_refresh', con=conn)
df_trr_trrstatus_refresh = pd.read_sql_query('select * from trr_trrstatus_refresh', con=conn)
df_trr_trr = pd.read_sql_query("select * from trr_trr", con=conn)
df_data_officer = pd.read_sql_query("select * from data_officer", con=conn)
df_data_policeunit = pd.read_sql_query("select * from data_policeunit", con=conn)

# ------Replace 'Redacted' values to None -------
df_trr_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)
df_trr_weapondischarge_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_weapondischarge_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)
df_trr_trr.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trrstatus_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trrstatus_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)

"************** TYPE CORRECTION **************"
print("***** Beginning type correction  *****")

# ----- Store the columns we need to typecast -----
trr_boolean_tables = ['officer_on_duty','officer_injured','officer_in_uniform', 'subject_armed','subject_injured', 'subject_alleged_injury',
                      'notify_oemc','notify_district_sergeant', 'notify_op_command','notify_det_division']
trr_boolean_weapon_tables =['firearm_reloaded','sight_used']
trr_integer_tables =['officer_age', 'beat', 'subject_birth_year', 'subject_age', 'officer_birth_year']

# ----- Add suffix column -----
df_trr_refresh['officer_suffix_name'] = add_suffix.add_suffix_column(df_trr_refresh,'officer_last_name')
df_trr_trrstatus_refresh['officer_suffix_name'] = add_suffix.add_suffix_column(df_trr_trrstatus_refresh,'officer_last_name')

# ----- Typecast boolean columns -----
for table in trr_boolean_tables:
    tc.convert_boolean(df_trr_refresh,table)
    df_trr_refresh[table] = df_trr_refresh[table].astype('bool')

for table in trr_boolean_weapon_tables:
    tc.convert_boolean(df_trr_weapondischarge_refresh,table)
    df_trr_weapondischarge_refresh[table] = df_trr_weapondischarge_refresh[table].astype('bool')

# ----- Typecast integers -----
for table in trr_integer_tables:
    tc.convert_integer(df_trr_refresh,table)
    # df_trr_refresh[table] = df_trr_refresh[table].astype('Int64')

# ----- Typecast timestamp -----
timestamp_created = tc.convert_timestamp(df_trr_refresh,'trr_created')
timestamp_datetime = tc.convert_timestamp(df_trr_refresh,'trr_datetime')
timestamp_status = tc.convert_timestamp(df_trr_trrstatus_refresh,'status_datetime')

# ----- Typecast dates -----
date_trr_app_date = tc.convert_date(df_trr_refresh,'officer_appointed_date')
date_trr_status_app_date = tc.convert_date(df_trr_trrstatus_refresh,'officer_appointed_date')
print("***** Type correction finished *****")

"************** RECONCILIATION **************"

print("***** Beginning reconciliation *****")
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
rec.reconcile_location(df_trr_refresh, 'location')

# Reconciliation indoor_or_outdoor
rec.reconcile_in_outdoor(df_trr_refresh, 'indoor_or_outdoor')

# Reconciliation party_fired_first [No change]

# Reconciliation subject weapon [No change]
print("***** Reconciliation finished *****")

"************** LINK OFFICER ID **************"

print("***** Beginning integration with Officer_id *****")

# Define the columns we want to match from each table
left_0 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_0 = ['first_name','last_name', 'gender', 'race', 'appointed_date', 'middle_initial','birth_year', 'suffix_name']

# Removing suffix_name
left_1 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date', 'officer_middle_initial','officer_birth_year']
right_1 = ['first_name','last_name', 'gender', 'race', 'appointed_date', 'middle_initial','birth_year']
# Removing birth_year
left_2 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date', 'officer_middle_initial', 'officer_suffix_name']
right_2 = ['first_name','last_name', 'gender', 'race', 'appointed_date', 'middle_initial', 'suffix_name']
# Removing middle_initial
left_3 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date','officer_birth_year', 'officer_suffix_name']
right_3 = ['first_name','last_name', 'gender', 'race', 'appointed_date','birth_year', 'suffix_name']
# Removing appointed_date
left_4 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_4 =['first_name','last_name', 'gender', 'race', 'middle_initial','birth_year', 'suffix_name']
# Removing race
left_5 = ['officer_first_name','officer_last_name', 'officer_gender','officer_appointed_date', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_5 = ['first_name','last_name', 'gender', 'appointed_date', 'middle_initial','birth_year', 'suffix_name']
# Removing gender
left_6 = ['officer_first_name','officer_last_name', 'officer_race','officer_appointed_date', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_6 = ['first_name','last_name', 'race', 'appointed_date', 'middle_initial','birth_year', 'suffix_name']
# Removing last_name
left_7 = ['officer_first_name', 'officer_gender', 'officer_race','officer_appointed_date', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_7 =['first_name', 'gender', 'race', 'appointed_date', 'middle_initial','birth_year', 'suffix_name']
# Removing first_name
left_8 = ['officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date', 'officer_middle_initial','officer_birth_year', 'officer_suffix_name']
right_8 = ['last_name', 'gender', 'race', 'appointed_date', 'middle_initial','birth_year', 'suffix_name']

# Iteration to merge both tables trying to match 7 fields
merged_df_refresh_1 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_1, right_on = right_1)
merged_df_status_1 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_1, right_on = right_1)

merged_df_refresh_2 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_2, right_on = right_2)
merged_df_status_2 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_2, right_on = right_2)

merged_df_refresh_3 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_3, right_on = right_3)
merged_df_status_3 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_3, right_on = right_3)

merged_df_refresh_4 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_4, right_on = right_4)
merged_df_status_4 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_4, right_on = right_4)

merged_df_refresh_5 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_5, right_on = right_5)
merged_df_status_5 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_5, right_on = right_5)

merged_df_refresh_6 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_6, right_on = right_6)
merged_df_status_6 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_6, right_on = right_6)

merged_df_refresh_7 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_7, right_on = right_7)
merged_df_status_7 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_7, right_on = right_7)

merged_df_refresh_8 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_8, right_on = right_8)
merged_df_status_8 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_8, right_on = right_8)


list_merged_refresh = [merged_df_refresh_1, merged_df_refresh_2, merged_df_refresh_3, merged_df_refresh_4, merged_df_refresh_5,
                       merged_df_refresh_6, merged_df_refresh_7, merged_df_refresh_8]
list_merged_status = [merged_df_status_1, merged_df_status_2, merged_df_status_3, merged_df_status_4, merged_df_status_5,
                      merged_df_status_6, merged_df_status_7, merged_df_status_8]

# ---- Merging results from all 8 rotations ----
huge_merged_refresh = pd.concat(list_merged_refresh, ignore_index=True)
huge_merged_status = pd.concat(list_merged_status, ignore_index=True)

# --- Drop duplicates from the dataframe containing results from merge -----
index_to_keep = huge_merged_refresh.astype(str).drop_duplicates().index
huge_merged_refresh = huge_merged_refresh.loc[index_to_keep]

index_to_keep = huge_merged_status.astype(str).drop_duplicates().index
huge_merged_status = huge_merged_status.loc[index_to_keep]

# ----- Filter out the matched rows -----
subset_merged_refresh_matched = huge_merged_refresh[huge_merged_refresh['id_y'].notna()]
subset_merged_status_matched = huge_merged_status[huge_merged_status['id'].notna()]

# ----- Filter out the unmatched rows -----
subset_merged_refresh_unmatched = huge_merged_refresh[~huge_merged_refresh['id_y'].notna()]
subset_merged_status_unmatched = huge_merged_status[~huge_merged_status['id'].notna()]

# ----- After performing matches in 7 rotations, we try using 5 fields to match the remaining unmatched rows -----
left_9 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date']
right_9 = ['first_name','last_name', 'gender', 'race', 'appointed_date']

merged_df_refresh_9 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_9, right_on = right_9)
merged_df_status_9 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_9, right_on = right_9)

# ----- Filter out the matched rows resulted from 7 rotations -----
id_x_matched = subset_merged_refresh_matched['id_x']
remaining = merged_df_refresh_9[~merged_df_refresh_9['id_x'].isin(id_x_matched)]

# ----- Join the matches from 7 rotations, matches from 5 column fields and the remaining unmatched rows ----
join_match_refresh = pd.concat([subset_merged_refresh_matched, remaining], ignore_index=True)
join_match_status = pd.concat([subset_merged_status_matched, merged_df_status_9], ignore_index=True)

# ----- Remove duplicates after merge and concat -----
join_match_refresh = join_match_refresh.loc[join_match_refresh.astype(str).drop_duplicates().index]

join_match_status = join_match_status.reset_index(drop=True)
join_match_status = join_match_status.replace({'None': '', 'nan': ''}, regex=True)
index_to_keep_3 = join_match_status.astype(str).drop_duplicates().index
join_match_status = join_match_status.loc[index_to_keep_3]

# ---- Calculate match rate for both the tables -----
# match_rate_refresh = (len(join_match_refresh) - join_match_refresh['id_y'].isna().sum())/len(join_match_refresh)
# match_rate_status = (len(join_match_status) - join_match_status['id'].isna().sum())/len(join_match_status)
#
# print(match_rate_refresh, " Refresh match initial rate")
# print(match_rate_status, " Status match initial rate")
print("***** Integration with Officer_id finished *****")

"************** LINK POLICE UNITS ID **************"
print("***** Beginning integration with Officer_unit_id and Officer_unit_detail_id *****")
# Transform the unit_name.data_policeunit to numbers
df_data_policeunit['unit_name'] = df_data_policeunit['unit_name'].astype('float64')
join_match_refresh['officer_unit_name'] = join_match_refresh['officer_unit_name'].astype('float64')
join_match_refresh['officer_unit_detail'] = join_match_refresh['officer_unit_detail'].astype('float64')

# Rename some columns to avoid conflict
join_match_refresh = join_match_refresh.rename(columns = {"id_y": "officer_id", "id_x": "id_main"})

# Merge the tables: trr_trr_refresh and data_policeunit by unit_name
merged_refresh_and_police = pd.merge(join_match_refresh, df_data_policeunit[['unit_name','id']], how = 'left', left_on = 'officer_unit_name', right_on = 'unit_name')

# Add office_unit_detail_id
merged_refresh_and_police = pd.merge(merged_refresh_and_police, df_data_policeunit[['unit_name','id']], how = 'left', left_on = 'officer_unit_detail', right_on = 'unit_name')
merged_refresh_and_police['officer_unit_name'] = merged_refresh_and_police['officer_unit_name'].astype('Int64')
merged_refresh_and_police['officer_id'] = merged_refresh_and_police['officer_id'].astype('Int64')

merged_refresh_and_police = merged_refresh_and_police.rename(columns={"id_main": "id", "id_x": "officer_unit_id", "id_y": "officer_unit_detail_id", "cr_number": "crid", "event_number": "event_id","notify_oemc": "notify_OEMC","notify_op_command": "notify_OP_command","notify_det_division": "notify_DET_division"})
keep_columns = ["id", "crid", "event_id", "beat", "block", "direction", "street", "location", "trr_datetime", "indoor_or_outdoor", "lighting_condition", "weather_condition", "notify_OEMC", "notify_district_sergeant", "notify_OP_command", "notify_DET_division", "party_fired_first", "officer_assigned_beat", "officer_on_duty", "officer_in_uniform", "officer_injured", "officer_rank", "subject_armed", "subject_injured", "subject_alleged_injury", "subject_age", "subject_birth_year", "subject_gender", "subject_race", "officer_id", "officer_unit_id", "officer_unit_detail_id", "point"]
trr = merged_refresh_and_police[keep_columns]

print("***** Integration with Officer_unit_id and Officer_unit_detail_id finished *****")

"************** CLEANING FORMAT **************"
print("***** Final format cleaning *****")
# Cleaning trr_refresh
merged_refresh_and_police = merged_refresh_and_police.rename(columns={"id_main": "id", "id_x": "officer_unit_id", "id_y": "officer_unit_detail_id", "cr_number": "crid", "event_number": "event_id","notify_oemc": "notify_OEMC","notify_op_command": "notify_OP_command","notify_det_division": "notify_DET_division"})
keep_columns = ["id", "crid", "event_id", "beat", "block", "direction", "street", "location", "trr_datetime", "indoor_or_outdoor", "lighting_condition", "weather_condition", "notify_OEMC", "notify_district_sergeant", "notify_OP_command", "notify_DET_division", "party_fired_first", "officer_assigned_beat", "officer_on_duty", "officer_in_uniform", "officer_injured", "officer_rank", "subject_armed", "subject_injured", "subject_alleged_injury", "subject_age", "subject_birth_year", "subject_gender", "subject_race", "officer_id", "officer_unit_id", "officer_unit_detail_id", "point"]
trr = merged_refresh_and_police[keep_columns]

# Cleaning trr_status
keep_columns_2 =["officer_rank", "star", "status", "status_datetime", "officer_id", "trr_id"]
join_match_status = join_match_status.rename(columns={"officer_star":"star", "id":"officer_id", "trr_report_id":"trr_id" })
trr_status = join_match_status[keep_columns_2]
trr_status = trr_status.rename(columns={"officer_rank":"rank"})

print("***** Verifying foreign keys *****")
"************** VERIFY FOREIGN KEYS **************"
df_actionresponse_refresh = pd.read_sql_query("select * from trr_actionresponse_refresh", con=conn)
df_subjectweapon_refresh = pd.read_sql_query("select * from trr_subjectweapon_refresh", con=conn)
df_charge_refresh = pd.read_sql_query("select * from trr_charge_refresh", con=conn)
# df_trr_weapondischarge_refresh
# df_trr_trrstatus_refresh


def verify_foreign_key(dataframe, table):
    dataframe = dataframe[dataframe[table].isin(trr['id'])]
    return(dataframe)

verified_actionresponse = verify_foreign_key(df_actionresponse_refresh, 'trr_report_id')
verified_subjectweapon = verify_foreign_key(df_subjectweapon_refresh, 'trr_report_id')
verified_charge = verify_foreign_key(df_charge_refresh, 'trr_report_id')
verified_weapondischarge = verify_foreign_key(df_trr_weapondischarge_refresh, 'trr_report_id')
verified_status = verify_foreign_key(df_trr_trrstatus_refresh, 'trr_report_id')

print(trr.columns)
print(verified_actionresponse.columns)
print(verified_charge.columns)
print(verified_weapondischarge.columns)
print(verified_status.columns)
print(verified_subjectweapon.columns)


"************** SAVE ALL THE FILES **************"

print("***** Downloading output into CSV files *****")

trr.to_csv('./output/trr-trr.csv', header=True, index= False, sep=',')
verified_charge.to_csv('./output/trr-charge.csv', header=True, index= False, sep=',')
verified_weapondischarge.to_csv('./output/trr-weapondischarge.csv', header=True, index= False, sep=',')
verified_status.to_csv('./output/trr-trrstatus.csv', header=True, index= False, sep=',')
verified_actionresponse.to_csv('./output/trr-actionresponse.csv', header=True, index= False, sep=',')
verified_subjectweapon.to_csv('./output/trr-subjectweapon.csv', header=True, index= False, sep=',')

conn.close()

print("***** Code ran successfully! *****")