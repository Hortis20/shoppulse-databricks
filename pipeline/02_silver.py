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

bronze_table = f"{catalog}.{schema}.shoppulse_bronze"
silver_table = f"{catalog}.{schema}.shoppulse_silver"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

df = spark.table(bronze_table)

# COMMAND ----------

df_typed = df.withColumn("transaction_date", coalesce(expr("try_to_timestamp(transaction_date)"),expr("try_to_timestamp(transaction_date, 'dd/MM/yyyy HH:mm:ss')")))

# COMMAND ----------


w = Window.partitionBy("transaction_id").orderBy(desc("ingestion_timestamp"))
df_dedup = df_typed.withColumn("rang",row_number().over(w)).filter(col("rang") == 1).drop("rang")


# COMMAND ----------

df_updates = df_dedup.drop("source_file","_rescued_data")

# COMMAND ----------

df_updates.createOrReplaceTempView("silver_updates")

# COMMAND ----------

spark.sql(f"""
  MERGE INTO {silver_table} AS t
  USING silver_updates AS s
  ON t.transaction_id = s.transaction_id

  WHEN MATCHED AND s.status <> 'COMPLETED'
    THEN DELETE

  WHEN MATCHED AND s.ingestion_timestamp > t.ingestion_timestamp
    THEN UPDATE SET *

  WHEN NOT MATCHED AND s.status = 'COMPLETED'
    THEN INSERT *
""")

# COMMAND ----------

