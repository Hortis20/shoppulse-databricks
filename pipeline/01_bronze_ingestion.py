# Databricks notebook source
# Création des widgets
dbutils.widgets.text(
    name="catalog",            # nom du widget : utilisé dans le code et par le job
    defaultValue="shoppulse",  # valeur utilisée quand on exécute à la main
    label="Catalog"            # texte affiché au-dessus du champ
)

dbutils.widgets.dropdown(
    name="schema",
    defaultValue="dev",
    choices=["dev", "prod"],   # les seules valeurs possibles
    label="Environnement"
)

# Lecture des valeurs
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

landing      = f"/Volumes/{catalog}/{schema}/raw/landing"
checkpoint   = f"/Volumes/{catalog}/{schema}/checkpoints/bronze"
bronze_table = f"{catalog}.{schema}.shoppulse_bronze"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_stream = (
    spark.readStream                                  # lecture en flux
         .format("cloudFiles")                        # = Auto Loader
         .option("cloudFiles.format", "json")         # format des fichiers sources
         .option("cloudFiles.inferColumnTypes", "true")  # deviner les vrais types
         .option("cloudFiles.schemaLocation", checkpoint) # où mémoriser le schéma
         .load(landing)
)

df_bronze = (
    df_stream
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", col("_metadata.file_path"))  # fichier d'origine
)

(
    df_bronze.writeStream
             .option("checkpointLocation", checkpoint)  # mémoire des fichiers lus
             .trigger(availableNow=True)                # traiter les nouveaux, puis s'arrêter
             .toTable(bronze_table)
)

# COMMAND ----------

