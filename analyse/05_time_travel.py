# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY shoppulse.prod.shoppulse_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'avant lot 2' AS etat,
# MAGIC        count(*)              AS nb_transactions,
# MAGIC        round(sum(amount), 2) AS ca_total
# MAGIC FROM shoppulse.prod.shoppulse_silver VERSION AS OF 1
# MAGIC UNION ALL
# MAGIC SELECT 'actuel',
# MAGIC        count(*),
# MAGIC        round(sum(amount), 2)
# MAGIC FROM shoppulse.prod.shoppulse_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM shoppulse.prod.shoppulse_silver
# MAGIC EXCEPT
# MAGIC SELECT * FROM shoppulse.prod.shoppulse_silver VERSION AS OF 1;

# COMMAND ----------

