import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    from_json,
    round as round_,
    struct,
    to_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

MY_NAME = "svitlana_moiseyenko"
BUILDING_SENSORS = f"{MY_NAME}_building_sensors"
ALERTS = f"{MY_NAME}_alerts"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONDITIONS_PATH = os.getenv("ALERTS_CONDITIONS_PATH", "alerts_conditions.csv")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/hw_06_alerts")

WINDOW_DURATION = "1 minute"
SLIDE_DURATION = "30 seconds"
WATERMARK_DELAY = "10 seconds"
NOT_USED = -999

spark = (
    SparkSession.builder.appName("SensorAlerts")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

reading_schema = StructType(
    [
        StructField("sensor_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
    ]
)

# 1. Потік показників датчиків із Kafka.
readings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", BUILDING_SENSORS)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), reading_schema).alias("reading"))
    .select("reading.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
)

# 2. Середні температура й вологість кожного датчика в ковзному вікні.
averages = (
    readings.withWatermark("timestamp", WATERMARK_DELAY)
    .groupBy(window("timestamp", WINDOW_DURATION, SLIDE_DURATION), "sensor_id")
    .agg(
        round_(avg("temperature"), 2).alias("t_avg"),
        round_(avg("humidity"), 2).alias("h_avg"),
    )
)

# 3. Умови алертів зі статичного файлу.
conditions = spark.read.csv(CONDITIONS_PATH, header=True, inferSchema=True)


def within(value, lower, upper):
    """Значення лежить у межах (lower, upper); -999 вимикає відповідну межу."""
    return ((col(lower) == NOT_USED) | (col(value) > col(lower))) & (
        (col(upper) == NOT_USED) | (col(value) < col(upper))
    )


# 4. Cross join середніх значень з умовами та фільтрація.
alerts = (
    averages.crossJoin(conditions)
    .where(
        within("t_avg", "temperature_min", "temperature_max")
        & within("h_avg", "humidity_min", "humidity_max")
    )
    .select(
        "window",
        "sensor_id",
        "t_avg",
        "h_avg",
        col("code").cast("string").alias("code"),
        "message",
        current_timestamp().alias("timestamp"),
    )
)

# 5. Запис алертів у вихідний топік Kafka.
query = (
    alerts.select(
        col("sensor_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", ALERTS)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Алерти: {BUILDING_SENSORS} -> {ALERTS}")
query.awaitTermination()
