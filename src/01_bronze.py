# Databricks notebook source
# создаём инфраструктуру (каталог, схема, volume)
spark.sql("""
CREATE CATALOG IF NOT EXISTS pipeline_1
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS pipeline_1.bronze
""")

spark.sql("""
CREATE VOLUME IF NOT EXISTS pipeline_1.bronze.landing_volume
""")

spark.sql("""
CREATE VOLUME IF NOT EXISTS pipeline_1.bronze.checkpoints_volume
""")

spark.sql("""
CREATE VOLUME IF NOT EXISTS pipeline_1.bronze.source_volume
""")

# пути и таблица
LANDING_VOLUME = "/Volumes/pipeline_1/bronze/landing_volume"
CHECKPOINT_PATH = "/Volumes/pipeline_1/bronze/checkpoints_volume/bronze_checkpoint"
SCHEMA_PATH = "/Volumes/pipeline_1/bronze/checkpoints_volume/bronze_schema"
BRONZE_TABLE = "pipeline_1.bronze.raw_events"

from pyspark.sql.functions import current_timestamp

# читаем данные (Auto Loader)
df = (
    spark.readStream
    .format("cloudfiles")
    .option("cloudfiles.format", "json")
    .option("cloudfiles.schemaLocation", SCHEMA_PATH)
    .option("cloudfiles.inferColumnTypes", "true")
    .load(LANDING_VOLUME)
    .withColumn("ingestion_ts", current_timestamp())
)

# пишем в bronze
(
    df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)