import pandas as pd
import sqlite3

# Connexion à la base de données SQLite
con = sqlite3.connect('my_data_1.db')
cur = con.cursor()

# Charger le fichier CSV dans un DataFrame pandas
df = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/labs/module_2/data/Spacex.csv")

# Insérer les données dans une table SQLite (remplacer la table si elle existe déjà)
df.to_sql("SPACEXTBL", con, if_exists='replace', index=False, method='multi')

# Supprimer la table si elle existe déjà
cur.execute("DROP TABLE IF EXISTS SPACEXTABLE;")
cur.execute("CREATE TABLE SPACEXTABLE AS SELECT * FROM SPACEXTBL WHERE Date IS NOT NULL;")

# Exécuter la requête pour obtenir les noms des sites de lancement uniques
cur.execute("SELECT DISTINCT Launch_Site FROM SPACEXTABLE;")

# Récupérer les résultats
launch_sites = cur.fetchall()

# Afficher les noms des sites de lancement uniques
print("Noms des sites de lancement uniques :")
for site in launch_sites:
    print(site[0])
    
# Display 5 records where launch sites begin with the string 'CCA' 
launch_sites_cca = cur.execute("SELECT * FROM SPACEXTABLE WHERE Launch_Site LIKE 'CCA%' LIMIT 5;" )   
# print("5 enregistrements où les sites de lancement commencent par 'CCA' :")
# for site in launch_sites_cca:
#     print(site)

# Display the total payload mass carried by boosters launched by NASA (CRS)
cur.execute("SELECT SUM(PAYLOAD_MASS__KG_) FROM SPACEXTABLE WHERE CUSTOMER = 'NASA (CRS)';")
total_payload_mass = cur.fetchall()[0][0]
# print("Masse totale de la charge utile transportée par les propulseurs lancés par la NASA (CRS) :", total_payload_mass, " kg")

#Display average payload mass carried by booster version F9 v1.1
cur.execute("SELECT AVG(PAYLOAD_MASS__KG_) FROM SPACEXTABLE WHERE BOOSTER_VERSION = 'F9 v1.1';")
average_payload_mass = cur.fetchall()[0][0]
# print("Masse moyenne de la charge utile transportée par la version du propulseur F9 v1.1 :", average_payload_mass, " kg")

# List the date when the first succesful landing outcome in ground pad was acheived.
cur.execute("SELECT MIN(DATE) FROM SPACEXTABLE WHERE Landing_Outcome = 'Success (ground pad)';")
first_successful_landing = cur.fetchall()[0][0]
# print("Date à laquelle le premier résultat de l'atterrissage réussi sur le coussin de sol a été obtenu :", first_successful_landing)

#List the names of the boosters which have success in drone ship and have payload mass greater than 4000 but less than 6000
boosters = cur.execute("SELECT DISTINCT Booster_Version FROM SPACEXTABLE WHERE Landing_Outcome='Success (drone ship)' AND PAYLOAD_MASS__KG_>4000 AND PAYLOAD_MASS__KG_<6000;")
# print("Booster with success drone ship")
# for booster in boosters:
#     print(booster[0])

#List the total number of successful and failure mission outcomes
cur.execute("SELECT COUNT(*) FROM SPACEXTABLE WHERE Landing_Outcome LIKE 'Success%';")
successful_outcomes = cur.fetchall()[0][0]
cur.execute("SELECT COUNT(*) FROM SPACEXTABLE WHERE Landing_Outcome LIKE 'Failure%';")
failed_outcomes = cur.fetchall()[0][0]
# print("Nombre total de résultats de mission réussis :", successful_outcomes)

#List the names of the booster_versions which have carried the maximum payload mass. Use a subquery
cur.execute("SELECT Booster_Version FROM SPACEXTABLE WHERE PAYLOAD_MASS__KG_ = (SELECT MAX(PAYLOAD_MASS__KG_) FROM SPACEXTABLE);")
max_payload_booster = cur.fetchall()
# print("Noms des versions de propulseur qui ont transporté la charge utile maximale :")
# for booster in max_payload_booster:
#     print(booster[0])

#List the records which will display the month names, failure landing_outcomes in drone ship ,booster versions, launch_site for the months in year 2015.
# Note: SQLLite does not support monthnames. So you need to use substr(Date, 6,2) as month to get the months and substr(Date,0,5)='2015' for year.
cur.execute("SELECT SUBSTR(Date, 6, 2) AS Month, Landing_Outcome, Booster_Version, Launch_Site FROM SPACEXTABLE WHERE SUBSTR(Date, 0, 5) = '2015' AND Landing_Outcome LIKE 'Failure (drone ship)%';")
# print("Enregistrements qui afficheront les noms des mois, les résultats d'atterrissage en échec sur le navire drone, les versions du propulseur, le site de lancement pour les mois de l'année 2015 :")
# for record in cur.fetchall():
#     print(record[0], record[1], record[2], record[3])

# Rank the count of landing outcomes (such as Failure (drone ship) or Success (ground pad)) between the date 2010-06-04 and 2017-03-20, in descending order.
cur.execute("SELECT Landing_Outcome, COUNT(*) AS Count FROM SPACEXTABLE WHERE Date BETWEEN '2010-06-04' AND '2017-03-20' GROUP BY Landing_Outcome ORDER BY Count DESC;")
# print("Classement du nombre de résultats d'atterrissage (tels que l'échec (navire drone) ou le succès (coussin de sol)) entre les dates 2010-06-04 et 2017-03-20, par ordre décroissant :")
# for record in cur.fetchall():
#     print(record[0], record[1])


# Valider et fermer la connexion
con.commit()
con.close()
