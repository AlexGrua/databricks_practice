spark.sql("""
CREATE SCHEMA IF NOT EXISTS pipeline_1.gold
""")

SILVER_TABLE = "pipeline_1.silver.silver_events"
CHECKPOINT_PATH = "/Volumes/pipeline_1/bronze/checkpoints_volume/gold_checkpoint"

GOLD_AVG = "pipeline_1.gold.metrics_avg"
GOLD_MIN = "pipeline_1.gold.metrics_min"
GOLD_MAX = "pipeline_1.gold.metrics_max"
GOLD_STDDEV = "pipeline_1.gold.metrics_stddev"

from pyspark.sql.types import NumericType
from pyspark.sql.functions import window, avg, min, max, stddev, current_timestamp, col, expr
from delta.tables import DeltaTable

silver_schema = spark.table(SILVER_TABLE).schema

numeric_cols = [
    field.name for field in silver_schema
    if isinstance(field.dataType, NumericType)
    and field.name not in ("device_id",)
]

df_silver = (
    spark.readStream
    .table(SILVER_TABLE)
)

def write_to_gold(microBatchDF, batchId):
    from pyspark.sql.functions import window, avg, min, max, stddev, current_timestamp, col, expr
    from delta.tables import DeltaTable

    safe_df = microBatchDF.filter(
        col("event_time") < expr("current_timestamp() - INTERVAL 2 MINUTES")
    )

    agg_exprs_avg = [avg(c).alias(c) for c in numeric_cols]
    agg_exprs_min = [min(c).alias(c) for c in numeric_cols]
    agg_exprs_max = [max(c).alias(c) for c in numeric_cols]
    agg_exprs_stddev = [stddev(c).alias(c) for c in numeric_cols]

    for agg_exprs, table_name in [
        (agg_exprs_avg, GOLD_AVG),
        (agg_exprs_min, GOLD_MIN),
        (agg_exprs_max, GOLD_MAX),
        (agg_exprs_stddev, GOLD_STDDEV)
    ]:
        agg_df = (
            safe_df
            .groupBy("device_id", window("event_time", "1 minute"))
            .agg(*agg_exprs)
            .withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .withColumn("inserted_at", current_timestamp())
            .drop("window")
        )

        if not spark.catalog.tableExists(table_name):
            agg_df.write.format("delta").mode("append").saveAsTable(table_name)
        else:
            delta_table = DeltaTable.forName(spark, table_name)
            (
                delta_table.alias("target")
                .merge(
                    agg_df.alias("source"),
                    "target.device_id = source.device_id AND target.window_start = source.window_start"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

(
    df_silver
    .writeStream
    .foreachBatch(write_to_gold)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)