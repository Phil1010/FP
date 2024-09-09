import numpy as np
import pandas as pd

df = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/dataset_part_1.csv")

df.isnull().sum()/len(df)*100

# calculate the number of lauches on each site
nb_launches = df['LaunchSite'].value_counts()

# calculate the number of occ of each orbit
nb_occ_orbit = df['Orbit'].value_counts()

# calculate number and occ of mission outcomes of the orbit
landing_outcomes = df['Outcome'].value_counts()

bad_outcome = set(landing_outcomes.keys()[[1,3,5,6,7]])

# create landing outcome label from outcome col, landing_class = 0 if bad_outcome, 1 otherwise
df['landing_class'] = df['Outcome'].apply(lambda x:0 if x in bad_outcome else 1)
df['Class'] = df['landing_class']
