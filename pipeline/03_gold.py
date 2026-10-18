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


silver_table = f"{catalog}.{schema}.shoppulse_silver"
gold_table   = f"{catalog}.{schema}.shoppulse_gold_user_metrics"

# COMMAND ----------

from pyspark.sql.functions import *
df_silver = spark.table(silver_table)
df_gold = df_silver.groupBy(col("user_id"),trunc("transaction_date", "month").alias("annee_mois")).agg(round(sum("amount"),2).alias("total_amount"),round(avg("amount"),2).alias("avg_basket"),count("transaction_id").alias("nb_orders"))


# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE VIEW {catalog}.{schema}.v_user_metrics AS
  SELECT
    user_id,
    annee_mois,
    total_amount AS chiffre_affaires,
    avg_basket   AS panier_moyen,
    nb_orders    AS nb_commandes
  FROM {gold_table}
""")