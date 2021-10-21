
# Checkpoint 2

## Pre-requisites

For this checkpoint we will be using Tableu, a software that is necessary to have installed to be able to run the .twb files. You can download it from the website: https://www.tableau.com/products/desktop/download

When a .twb file is opened, you will need to connect to the cdpd database with the following credentials:

user: cpdbstudent
pw: DataSci4AI

## Visualization


### 1. Bar graph of civilian unsustained allegations in terms of years in the force of police officer

To visualize the bar graph, first execute the Postgresql query in [Allegations_vs_Years.sql](https://github.com/AneryPatel/TheHappyMammoths/tree/main/Checkpoint%202/src#:~:text=Allegations_vs_Years.sql) to generate a table "years_versus_allegations". 

Then proceed to run the visualization in Tableau given in the src file [UnsustainedCivilianAllegations_vs_YearsinForce.twb](https://github.com/AneryPatel/TheHappyMammoths/tree/main/Checkpoint%202/src#:~:text=UnsustainedCivilianAllegations_vs_YearsinForce.twb). 


### 2. Heat map of neighborhoods that represent concentration of civilian unsustained allegations per capita when viewed in tandem with the median income of that area

 
To solve this task, first execute the Postgresql query in the file [Unsustained_percapita_table.sql](https://github.com/AneryPatel/TheHappyMammoths/blob/main/Checkpoint%202/src/Median_income_heatmap.twb) to generate a table in the database.

Then proceed to visualize the heatmap of unsustained civilian allegations per capita in each neighborhood in Tableau as given in the src file [Unsustained_allegations_percapita2.twb](https://github.com/AneryPatel/TheHappyMammoths/tree/main/Checkpoint%202/src#:~:text=Unsustained_allegations_percapita2.twb).

Lastly, to visualize the heatmap of median income per neighborhood (to be viewed alongside the previous visualization), run the file [Median_income_heatmap.twb](https://github.com/AneryPatel/TheHappyMammoths/tree/main/Checkpoint%202/src#:~:text=Median_income_heatmap.twb) in Tableau.




