
# Data Cleaning and Integration

For this project we updated a Tactical Response Reports in CPDB. In this assignment we will be adding reports through the mid 2020s in this assignment. The several types of cleaning and integration that we needed for this assignment. It includes type correction, typos, capitalization errors, linking entities to IDs, lining up values with their domains for foreign key relationships. The goal here is to be able to insert the output of your homework into the TRR table as a seamless addition to our existing data on this topic.


## Setup
In this assignment, we used Python 3.9. The libraries that we used in order to clean and integrate our data was ```pandas``` (version 0.24.0 or greater) and ```psycopg2```. Our database depends on the postgres database.
The psycopg2 and postgre database were installed hand-in-hand. This was the way we were able to use sql queries in a a Python script. We used a ```PostgreSQL``` database cluster in our code. The psycopg2 adapter for PostgreSQL makes it easy to get connected to a database with just a few lines of code


## Data Cleaning
### Type-correction

For our Data Cleaning of our database we first cleaned the data to line it up with already exsisting records. We first started with the process of type correcting our data. We started to type correct by importing ```psycopg2``` and ```pandas``` libraries in order to type-correct our data. We corrected our datatypes from trr. We want to make the Yes or No values into boolean values that of 0 or 1. We convert the integers, timestamps and the dates in order for them to match the data in the original tables

You can find this code in ```type_correction.py```, that will be automatically run by the ```main.py```

### Reconciliation

We reconcile each of the data sections that include the subject's race, officer's race, gender, birth year, first name, last name, locations, and streets. For each of these sections we reconciled each of these topics from the following tables of ```df_trr_trr_refresh```, ```df_trr_weapondischarge_refresh```, ```df_trr_trrstatus_refresh```, ```df_trr_trr```, ```df_data_officer``` and converting them to dataframe in order to ultimately reconcile the data.

You can find this code in ```reconciliation.py```, that will be automatically run by the ```main.py```


## Data Integration

### Linking the Officer Ids

We needed to match the officers filing the TRRs (data in ```trr_trr_refresh```) and the ones updating the status of a TRR (in ```trr_trrstatus_refresh```) with their IDs in data_officer. In order to achieve this measure, we first defined the columns that we want to match so the program will know which table to link for the specifc id. We then want to remove the suffix names from left to right for each table column. We then iterated both tables trying to match the 7 fields. We then merge the results from all 8 rotations, drop the duplicates from the dataframe containing results from the merge. We then filter out the matched rows and unmatched rows. After we have performed matches in 7 rotations, we try using five fields to match the remaining unmatched rows and then filter out the matched rows from the 7 rotations. We then join the matches from 7 rotations, the matches from the 5 columns and the remaning unmatched rows. We then need to remove duplicates after the merge and concatination. At last we calculate the match rate for both the tables to finsh the linking of the officer Ids.

### Linking Police Units ID

We first start by transforming the ```unit_name.data_policeunit``` to numbers by joining the matched officer unit detail data and then merge the tables of ```trr_trr_refresh``` and ```data_policeunit``` by ```unit_name```. This will then makeup our newly merged officer unit detail id.

### Cleaning Format

From our previous steps we are allowed to clean our trr_refresh and trr_status tables.

## Foreign key verification

For our Foreign Key Verification specifically in python we looked to pare our trr_trr_refresh table with our ID column. We then called the pairing IDs and then checked if all the values in the tables exisisted in IDs and deleted the rows that did not exisit.
