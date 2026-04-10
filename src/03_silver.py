# Databricks notebook source
spark.sql("""
CREATE SCHEMA IF NOT EXISTS pipeline_1.silver
""")

from datetime import datetime
from pyspark.sql.functions import explode, col, current_timestamp, substring

MIN_DATE = '2025-01-01'
MAX_DATE = datetime.now().strftime('%Y-%m-%d')

BRONZE_TABLE = "pipeline_1.bronze.raw_events"
SILVER_TABLE = "pipeline_1.silver.silver_events"
CHECKPOINT_PATH = "/Volumes/pipeline_1/bronze/checkpoints_volume/silver_checkpoint"

df_bronze = spark.readStream.table(BRONZE_TABLE)

df_exploded = (
    df_bronze
    .select(explode(col("values")).alias("metric_row"))
    .select(
        col("metric_row.device_id").alias("device_id"),
        col("metric_row.metric").alias("metric"),
        col("metric_row.value").alias("value"),
        (substring(col("metric_row.timestampUtc").cast("string"), 1, 13).cast("long") / 1000).cast("timestamp").alias("event_time")
    )
    .filter(f"event_time > '{MIN_DATE}' AND event_time < '{MAX_DATE}'")
)

def write_to_silver(microBatchDF, batchId):
    pivoted_df = (
        microBatchDF
        .groupBy("device_id", "event_time")
        .pivot("metric")
        .agg({"value": "first"})
    )

    type_mapping = {
        "Wind_speed": "float",
        "AirPres": "float",
        "Ambient_temperature": "float",
        "Humidity": "float",
        "Power_output": "float",
        "Rotor_rpm": "int",
        "Error_code": "int",
        "Brake_active": "boolean",
        "Grid_connected": "boolean",
        "Operation_mode": "string"
    }

    select_exprs = []
    for c in pivoted_df.columns:
        if c in type_mapping:
            select_exprs.append(col(c).cast(type_mapping[c]).alias(c))
        else:
            select_exprs.append(col(c))

    result_df = (
        pivoted_df
        .select(*select_exprs)
        .withColumn("inserted_at", current_timestamp())
    )

    (
        result_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )

(
    df_exploded
    .writeStream
    .foreachBatch(write_to_silver)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)
# Databricks notebook source
spark.sql("""
CREATE SCHEMA IF NOT EXISTS pipeline_1.silver
""")

from datetime import datetime
from pyspark.sql.functions import explode, col, current_timestamp, substring

MIN_DATE = '2025-01-01'
MAX_DATE = datetime.now().strftime('%Y-%m-%d')

BRONZE_TABLE = "pipeline_1.bronze.raw_events"
SILVER_TABLE = "pipeline_1.silver.silver_events"
CHECKPOINT_PATH = "/Volumes/pipeline_1/bronze/checkpoints_volume/silver_checkpoint"

df_bronze = spark.readStream.table(BRONZE_TABLE)

df_exploded = (
    df_bronze
    .select(explode(col("values")).alias("metric_row"))
    .select(
        col("metric_row.device_id").alias("device_id"),
        col("metric_row.metric").alias("metric"),
        col("metric_row.value").alias("value"),
        (substring(col("metric_row.timestampUtc").cast("string"), 1, 13).cast("long") / 1000).cast("timestamp").alias("event_time")
    )
    .filter(f"event_time > '{MIN_DATE}' AND event_time < '{MAX_DATE}'")
)

def write_to_silver(microBatchDF, batchId):
    pivoted_df = (
        microBatchDF
        .groupBy("device_id", "event_time")
        .pivot("metric")
        .agg({"value": "first"})
    )

    type_mapping = {
        "Wind_speed": "float",
        "AirPres": "float",
        "Ambient_temperature": "float",
        "Humidity": "float",
        "Power_output": "float",
        "Rotor_rpm": "int",
        "Error_code": "int",
        "Brake_active": "boolean",
        "Grid_connected": "boolean",
        "Operation_mode": "string"
    }

    select_exprs = []
    for c in pivoted_df.columns:
        if c in type_mapping:
            select_exprs.append(col(c).cast(type_mapping[c]).alias(c))
        else:
            select_exprs.append(col(c))

    result_df = (
        pivoted_df
        .select(*select_exprs)
        .withColumn("inserted_at", current_timestamp())
    )

    (
        result_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )

(
    df_exploded
    .writeStream
    .foreachBatch(write_to_silver)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)