import psycopg2
import numpy as np
import pandas as pd
import type_correction as tc
import reconcilation as rec
import add_suffix as add_suffix

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
df_data_policeunit = pd.read_sql_query("select * from data_policeunit", con=conn)

# Replace 'Redacted' values to None
df_trr_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)
df_trr_weapondischarge_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_weapondischarge_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)
df_trr_trr.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trrstatus_refresh.replace(to_replace = 'Redacted', value = None, inplace = True)
df_trr_trrstatus_refresh.replace(to_replace = 'REDACTED', value = None, inplace = True)

# List with columns by type
trr_boolean_tables = ['officer_on_duty','officer_injured','officer_in_uniform', 'subject_armed','subject_injured', 'subject_alleged_injury',
                      'notify_oemc','notify_district_sergeant', 'notify_op_command','notify_det_division']
trr_boolean_weapon_tables =['firearm_reloaded','sight_used']
trr_integer_tables =['officer_age', 'beat', 'subject_birth_year', 'subject_age', 'officer_birth_year']

"************** TYPE CORRECTION **************"
# Add suffix column
df_trr_refresh['officer_suffix_name'] = add_suffix.add_suffix_column(df_trr_refresh,'officer_last_name')
df_trr_trrstatus_refresh['officer_suffix_name'] = add_suffix.add_suffix_column(df_trr_trrstatus_refresh,'officer_last_name')

# Convert booleans
for table in trr_boolean_tables:
    tc.convert_boolean(df_trr_refresh,table)

for table in trr_boolean_weapon_tables:
    tc.convert_boolean(df_trr_weapondischarge_refresh,table)

# Convert integers
for table in trr_integer_tables:
    tc.convert_integer(df_trr_refresh,table)

# Convert timestamp
timestamp_created = tc.convert_timestamp(df_trr_refresh,'trr_created')
timestamp_datetime = tc.convert_timestamp(df_trr_refresh,'trr_datetime')
timestamp_status = tc.convert_timestamp(df_trr_trrstatus_refresh,'status_datetime')

# Convert dates
date_trr_app_date = tc.convert_date(df_trr_refresh,'officer_appointed_date')
date_trr_status_app_date = tc.convert_date(df_trr_trrstatus_refresh,'officer_appointed_date')


"************** RECONCILIATION **************"

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


"************** LINK OFFICER ID **************"

# Define the columns we want to match from each table
# MATCH USING 7 ROTATING FIELDS
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

huge_merged_refresh = pd.concat(list_merged_refresh)
huge_merged_status = pd.concat(list_merged_status)

# We save just the matched on dataframe (id_y != None)
subset_merged_refresh_matched = huge_merged_refresh[huge_merged_refresh['id_y'].notna()]
subset_merged_status_matched = huge_merged_status[huge_merged_status['id'].notna()]

# Then, we remove the duplicates from this matched dataframe
index_to_keep = subset_merged_refresh_matched.astype(str).drop_duplicates().index
index_to_keep_2 = subset_merged_status_matched.astype(str).drop_duplicates().index
print(index_to_keep_2)
print(len(subset_merged_status_matched))

# Filter by the index to keep (NOT DOING ANYTHING)
reduced_merged_refresh_matched = subset_merged_refresh_matched.loc[index_to_keep]
reduced_merged_status_matched = subset_merged_status_matched.loc[index_to_keep_2]
print(len(subset_merged_status_matched.loc[index_to_keep_2]))
#print(reduced_merged_refresh_matched)

# After having the match using 7 rotating fields, we also want to apply the matching using the 5 main fields
left_9 = ['officer_first_name','officer_last_name', 'officer_gender', 'officer_race','officer_appointed_date']
right_9 = ['first_name','last_name', 'gender', 'race', 'appointed_date']

merged_df_refresh_9 = pd.merge(df_trr_refresh, df_data_officer, how = 'left', left_on = left_9, right_on = right_9)
merged_df_status_9 = pd.merge(df_trr_trrstatus_refresh, df_data_officer, how = 'left', left_on = left_9, right_on = right_9)

# We remove the rows that are repeated in the merge of 7 fields (already matched)
id_x_matched = reduced_merged_refresh_matched['id_x']
remaining = merged_df_refresh_9[~merged_df_refresh_9['id_x'].isin(id_x_matched)]

# VERIFICAR INDEX THE ID NOT NULL
#id_x_matched_2 = reduced_merged_status_matched['id']
#remaining_2 = merged_df_status_9[~merged_df_status_9['id'].isin(id_x_matched_2)]

# Now we join the output of the matches using 7 rotating fields and the remaining using 5 fields
join_match_refresh = pd.concat([reduced_merged_refresh_matched,remaining])
#join_match_status = pd.concat([reduced_merged_status_matched,remaining_2])

#print(len(remaining_2))
#print(len(reduced_merged_status_matched))
#print(join_match_status)


#match_rate_refresh = (len(merged_df_refresh) - merged_df_refresh['id_y'].isna().sum())/len(merged_df_refresh)
#match_rate_status = (len(merged_df_status) - merged_df_status['id'].isna().sum())/len(merged_df_status)

#print(match_rate_refresh, " Refresh match initial rate")
#print(match_rate_status, " Status match initial rate")



"************** LINK POLICE UNITS ID **************"

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

# Rename some columns again
merged_refresh_and_police = merged_refresh_and_police.rename(columns = {"id_main": "id", "id_x": "officer_unit_id", "id_y": "officer_unit_detail_id"})

# Delete columns not relevant for trr
columns_to_delete_refresh = ['first_name', 'middle_initial', 'last_name','suffix_name', 'gender', 'race', 'appointed_date', 'birth_year',
                          'officer_last_name', 'officer_first_name','officer_middle_initial','officer_gender','officer_race','officer_age',
                          'officer_appointed_date','officer_birth_year','officer_unit_name', 'officer_unit_detail', 'trr_created','latitude',
                          'longitude', 'rank','active','tags', 'resignation_date', 'complaint_percentile', 'middle_initial2', 'civilian_allegation_percentile',
                          'honorable_mention_percentile','internal_allegation_percentile','trr_percentile','allegation_count', 'sustained_count', 'civilian_compliment_count',
                          'current_badge','current_salary', 'discipline_count', 'honorable_mention_count', 'last_unit_id', 'major_award_count', 'trr_count',
                          'unsustained_count', 'has_unique_name', 'created_at', 'updated_at', 'unit_name_x','unit_name_y', 'officer_suffix_name']

merged_refresh_and_police = merged_refresh_and_police.drop(columns_to_delete_refresh, axis=1)

#merged_df_status_2 = merged_df_status_2.drop(['first_name', 'middle_initial', 'last_name','suffix_name', 'gender', 'race', 'appointed_date', 'birth_year'], axis=1)

# Save the final merged table in a CSV
merged_refresh_and_police.to_csv('Integration_trr_refresh_and_police.csv', header=True, index= False, sep=',')
#join_match_status.to_csv('Integration_trr_status_final.csv', header=True, index= False, sep=',')
#merged_df_status_9.to_csv('Integration_trr_status_5fields.csv', header=True, index= False, sep=',')

conn.close()