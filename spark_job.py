import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round

# 1. Initialisation de la session Spark
print("Démarrage de Spark...")
spark = SparkSession.builder \
    .appName("SportData_Consolidation") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
    .getOrCreate()

# 2. Extraction des référentiels
print("Lecture des fichiers Excel (Référentiels)...")
pdf_rh = pd.read_excel("Données+RH.xlsx")

# Nettoyage des noms de colonnes
pdf_rh.columns = [c.replace(' ', '_').replace("'", "") for c in pdf_rh.columns]

# CORRECTION WINDOWS 1970 : On convertit les dates en chaînes de caractères
for col_name in pdf_rh.select_dtypes(include=['datetime64', 'datetimetz']).columns:
    pdf_rh[col_name] = pdf_rh[col_name].astype(str)

df_rh = spark.createDataFrame(pdf_rh)

# 3. Extraction de la base de données (Historique Strava)
print("Lecture de PostgreSQL (Activités)...")
df_activites = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/strava_poc") \
    .option("dbtable", "activites_sportives") \
    .option("user", "admin") \
    .option("password", "adminpassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# 4. Transformation : Calcul des 5 jours de bien-être
# Règle : au minimum 15 activités physiques dans l'année.
print("Calcul des activités par salarié...")
df_stats = df_activites.groupBy("id_salarie") \
    .agg(count("*").alias("nombre_activites"))

df_jours_bien_etre = df_stats.withColumn(
    "jours_bien_etre_acquis",
    when(col("nombre_activites") >= 15, 5).otherwise(0)
)

# 5. Transformation : Jointure et Calcul de la prime (5%)
print("Consolidation des données...")
# Jointure entre les RH et les statistiques sportives (Left Join)
df_final = df_rh.join(df_jours_bien_etre, df_rh["ID_salarié"] == df_jours_bien_etre["id_salarie"], "left")

# On remplace les valeurs nulles par 0 (pour ceux qui ne font pas de sport)
df_final = df_final.fillna({"nombre_activites": 0, "jours_bien_etre_acquis": 0})

# Règle : Prime de 5% du salaire brut pour les trajets sportifs validés
modes_sportifs = ['Marche/running', 'Vélo/Trottinette/Autres']

df_final = df_final.withColumn(
    "prime_sportive_euros",
    when(col("Moyen_de_déplacement").isin(modes_sportifs), round(col("Salaire_brut") * 0.05, 2))
    .otherwise(0)
)

df_final = df_final.withColumn(
    "cout_total_entreprise",
    col("Salaire_brut") + col("prime_sportive_euros")
)

# 6. Chargement (Load) : Export pour PowerBI
print("Exportation vers un fichier CSV pour PowerBI...")
# En production, on utiliserait le format Delta (.format("delta")). 
# Pour faciliter l'import PowerBI du POC, on utilise un CSV consolidé.
df_final.select(
    "ID_salarié", "Nom", "Prénom", "BU", "Type_de_contrat", "Moyen_de_déplacement",
    "Salaire_brut", "prime_sportive_euros", "cout_total_entreprise", 
    "nombre_activites", "jours_bien_etre_acquis"
).toPandas().to_csv("powerbi_dataset.csv", index=False)

print("Traitement terminé avec succès. Le fichier 'powerbi_dataset.csv' est prêt.")
spark.stop()