# Checkpoint 4

In this checkpoint we used ```PySpark```, ```Pandas``` and ```GraphFrames``` modules from Python to do the Graph Analytics. You can find the code in the ```src``` folder and also online in the link below.

## Prerequisites

The project runs in Google Colab, that is a Jupyter notebook environment that just requires a google account to access. You can find the code here:
https://colab.research.google.com/drive/1Z4AiE-lTI8DfBbKh2S2kxpbIxHhkF2C2?authuser=1#scrollTo=lh0VIfBFZBjq

To be able to run the code, you have to download the graphframes jar file from ```Graphframe jar file``` (inside Colab) and upload it in the Google Colab Files folder. Can be found in the left pane of the main window.

To run the code, just go to: Runtime -> Run all. If you have any troubles executing the code, go to: Runtime -> Restart run time, and run it again.

## Graph analytics questions

1. Network analysis: Is there a connection between investigators and certain officers or districts who are involved in incidents that have unsustained complaints using connected components to test if there is a correlation between the two.

2. Centrality analysis: Where are the most influential (negatively) people in the CPD network? where the most unsustained reports are concentrated? We want to track the cascading effect between different police officers. Does one officer with a high number of complaints rub off his culture and behavior on another officer who has a relatively low number of complaints against him?

Our motivations and conclusions regarding this analysis can be found in the ```findings.pdf``` in the root directory.
