# Databricks notebook source
dbutils.widgets.text(
    name="catalog",
    defaultValue="shoppulse",
    label="Catalog"
)

dbutils.widgets.dropdown(
    name="schema",
    defaultValue="dev",
    choices=["dev", "prod"],
    label="Environnement"
)

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

# COMMAND ----------

schema_full     = f"{catalog}.{schema}"
raw_volume      = f"{catalog}.{schema}.raw"
checkpoints_vol = f"{catalog}.{schema}.checkpoints"
silver_table    = f"{catalog}.{schema}.shoppulse_silver"

# COMMAND ----------


# creation schema 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_full}")

#  volumes
spark.sql(f"CREATE VOLUME IF NOT EXISTS {raw_volume}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {checkpoints_vol}")

# creation landing
dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/raw/landing")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {silver_table} (
    transaction_id      STRING,
    user_id             STRING,
    product_id          STRING,
    amount              DOUBLE,
    transaction_date    TIMESTAMP,
    status              STRING,
    ingestion_timestamp TIMESTAMP
  )
""")