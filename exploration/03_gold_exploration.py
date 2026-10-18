# Databricks notebook source
from pyspark.sql.functions import *
df_silver = spark.table("shoppulse.dev.shoppulse_silver")
df_gold = df_silver.groupBy(col("user_id"),trunc("transaction_date", "month").alias("annee_mois")).agg(round(sum("amount"),2).alias("total_amount"),round(avg("amount"),2).alias("avg_basket"),count("transaction_id").alias("nb_orders"))


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS shoppulse.dev.shoppulse_gold_user_metrics;

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").saveAsTable("shoppulse.dev.shoppulse_gold_user_metrics")

# COMMAND ----------

df_gold.count()

# COMMAND ----------

