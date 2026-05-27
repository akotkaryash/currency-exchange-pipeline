from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lag, stddev, avg, min, max, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CurrencyStreamConsumer") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("base", StringType(), True),
    StructField("date", StringType(), True),
    StructField("rates", MapType(StringType(), DoubleType()), True)
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "currency_rates") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

currencies = ["AUD", "CAD", "EUR", "GBP", "INR", "JPY", "SGD"]

flattened_stream = parsed_stream.select(
    col("timestamp"),
    col("base"),
    col("date"),
    *[col("rates")[currency].alias(currency) for currency in currencies]
)

def  process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    unpivoted = batch_df.selectExpr(
        "timestamp",
        "base",
        "stack(7, 'AUD', AUD, 'CAD', CAD, 'EUR', EUR, 'GBP', GBP, 'INR', INR, 'JPY', JPY, 'SGD', SGD) as (target_currency, rate)"
    )

    window_spec = Window.partitionBy("target_currency").orderBy("timestamp")

    rate_change = unpivoted.withColumn(
        "prev_rate", lag("rate", 1).over(window_spec)
    ).withColumn(
        "rate_change", (col("rate") - col("prev_rate"))
    ).withColumn(
        "pct_change", (col("rate_change") / col("prev_rate")) * 100
    )

    # (rate_change.write
    #     .format("bigquery")
    #     .option("table", "de-weather-project-492917.currency_processed.currency_rates")
    #     .option("writeMethod", "direct")
    #     # .option("temporaryGcsBucket", "de-weather-project-492917-temp")
    #     .mode("append")
    #     .save())

    (rate_change.write
        .mode("append")
        .parquet("/tmp/currency_rates"))

query = flattened_stream.writeStream.foreachBatch(process_batch).option("checkpointLocation", "/tmp/checkpoint").start()

query.awaitTermination()