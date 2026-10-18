# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

df = spark.table("shoppulse.dev.shoppulse_bronze")

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.limit(10).show()

# COMMAND ----------

#date_iso = F.expr("try_to_timestamp(transaction_date)")
#date_fr  = F.expr("try_to_timestamp(transaction_date, 'dd/MM/yyyy HH:mm:ss')")

df_typed = df.withColumn("transaction_date", coalesce(expr("try_to_timestamp(transaction_date)"),expr("try_to_timestamp(transaction_date, 'dd/MM/yyyy HH:mm:ss')")))

# COMMAND ----------

df_typed.printSchema()
df_typed.filter(col("transaction_date").isNull()).count()


# COMMAND ----------

df_typed.limit(10).show()

# COMMAND ----------


w = Window.partitionBy("transaction_id").orderBy(desc("ingestion_timestamp"))
df_dedup = df_typed.withColumn("rang",row_number().over(w)).filter(col("rang") == 1).drop("rang")

df_dedup.printSchema()
df_dedup.count()


# COMMAND ----------

df_clean = df_dedup.filter(col("status") == "COMPLETED")
df_clean.count()

# COMMAND ----------

df_updates = df_clean.drop("source_file","_rescued_data")

# COMMAND ----------

df_updates.createOrReplaceTempView("silver_updates")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO shoppulse.dev.shoppulse_silver AS t          -- t = la table cible (target)
# MAGIC USING silver_updates AS s                                -- s = les nouvelles données (source)
# MAGIC ON t.transaction_id = s.transaction_id                   -- la clé de comparaison
# MAGIC
# MAGIC WHEN MATCHED AND s.ingestion_timestamp > t.ingestion_timestamp
# MAGIC   THEN UPDATE SET *                                      -- clé trouvée ET version plus récente → mise à jour
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *                                          -- clé absente → insertion

# COMMAND ----------

df_updates = df_dedup.drop("source_file","_rescued_data")

# COMMAND ----------

df_updates.createOrReplaceTempView("silver_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, count(*) FROM silver_updates GROUP BY status;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO shoppulse.dev.shoppulse_silver AS t
# MAGIC USING silver_updates AS s
# MAGIC ON t.transaction_id = s.transaction_id
# MAGIC
# MAGIC WHEN MATCHED AND s.status <> 'COMPLETED'
# MAGIC   THEN DELETE
# MAGIC
# MAGIC WHEN MATCHED AND s.ingestion_timestamp > t.ingestion_timestamp
# MAGIC   THEN UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED AND s.status = 'COMPLETED'
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, count(*) FROM silver_updates GROUP BY status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'v0' AS version, count(*) AS nb FROM shoppulse.dev.shoppulse_silver VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'v1', count(*) FROM shoppulse.dev.shoppulse_silver VERSION AS OF 1
# MAGIC UNION ALL
# MAGIC SELECT 'actuelle', count(*) FROM shoppulse.dev.shoppulse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM shoppulse.dev.shoppulse_silver VERSION AS OF 3
# MAGIC UNION ALL
# MAGIC SELECT count(*) FROM shoppulse.dev.shoppulse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(amount) FROM shoppulse.dev.shoppulse_silver VERSION AS OF 3
# MAGIC UNION ALL
# MAGIC SELECT sum(amount) FROM shoppulse.dev.shoppulse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM shoppulse.dev.shoppulse_silver
# MAGIC EXCEPT
# MAGIC SELECT * FROM shoppulse.dev.shoppulse_silver VERSION AS OF 3

# COMMAND ----------

