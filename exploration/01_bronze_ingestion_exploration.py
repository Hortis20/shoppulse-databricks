# Databricks notebook source
df = spark.read.json("/Volumes/shoppulse/dev/raw/landing")

# COMMAND ----------

df.printSchema

# COMMAND ----------

df.limit(10).show()

# COMMAND ----------

df.count()

# COMMAND ----------

#from pyspark.sql import functions as sf
#df_bronze =df.withColumn("ingestion_timestamp",sf.current_timestamp())

# COMMAND ----------

#df_bronze.write.format("delta").mode("append").saveAsTable("shoppulse.dev.shoppulse_bronze")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

landing    = "/Volumes/shoppulse/dev/raw/landing"
checkpoint = "/Volumes/shoppulse/dev/checkpoints/bronze"

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
             .toTable("shoppulse.dev.shoppulse_bronze")
)

# COMMAND ----------

df_bronze.count()

# COMMAND ----------

