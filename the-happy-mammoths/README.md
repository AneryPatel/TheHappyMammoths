
# Data Cleaning and Integration

For this project we updated a Tactical Response Reports in CPDB. In this assignment we will be adding reports through the mid 2020s in this assignment. The several types of cleaning and integration that we needed for this assignment. It includes type correction, typos, capitalization errors, linking entities to IDs, lining up values with their domains for foreign key relationships. The goal here is to be able to insert the output of your homework into the TRR table as a seamless addition to our existing data on this topic.


## Setup
In this assignment, we used Python 3.9. The libraries that we used in order to clean and integrate our data was pandas, numpy, psycopg2, and type_correction. Our database depends on the postgres database.

We must create a dataframe from our trr_refresh database and a dataframe out of the data_officer table

We must match the trr_refresh database to the officer data in order to
## Data Cleaning
Type-corecting

For our Data Cleaning of our database we first cleaned the data to line it up with already exsisting records. We first started with the process of type correcting our data. We started to type correct by importing datetime, psycopg2, and pandas libraries in order to type-correct our data. We corrected our datatypes from trr. We want to make the Yes or No values into boolean values that of 0 or 1. We convert the integers, timestamps and the dates in order for them to match the data in the original tables

Reconciliation

We reconcile each of the data sections that include the subject's race, officer's race, gender, birth year, first name, last name, locations, and streets. For each of these sections we reconciled each of these topics from the following tables of df_trr_trr_refresh,df_trr_weapondischarge_refresh,df_trr_trrstatus_refresh,df_trr_trr,df_data_officer and converting them to dataframe in order to ultimately reconcile the data.

## Data Integration

Linking the Officer Ids

We needed to match the officers filing the TRRs (data in trr_trr_refresh) and the ones updating the status of a TRR (in trr_trrstatus_refresh) with their IDs in data_officer. In order to achieve this measure, we first defined the columns that we want to match so the program will know which table to link for the specifc id.
