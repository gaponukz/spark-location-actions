from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_string, regexp_extract, window

spark: SparkSession = SparkSession.builder.appName("User actions stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

users = spark.read.format("org.apache.spark.sql.json").load("./users.json")

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "actions")
    .load()
).selectExpr("CAST(value AS STRING)")

parsed_stream = stream.select(
    regexp_extract(col("value"), r"user_id: (\d+)", 1).alias("user_id"),
    regexp_extract(col("value"), r"action: (\w+)", 1).alias("action"),
    regexp_extract(col("value"), r"timestamp: ([\d\-T\:\.]+)", 1)
    .cast("timestamp")
    .alias("timestamp"),
)

users_df = users.select(
    users["id"],
    format_string("%s, %s", users["country"], users["city"]).alias("location"),
)

joined_df = parsed_stream.filter(parsed_stream["action"].isin("submit", "click")).join(
    users_df, parsed_stream["user_id"] == users_df["id"], "left"
)

df = joined_df.where(joined_df["location"].isNotNull())
null_df = joined_df.where(joined_df["location"].isNull())

null_counts = (
    null_df
    .withWatermark("timestamp", "5 minutes")
    .groupBy(window(col("timestamp"), "5 minutes")) 
    .count()
)

windowed_counts = (
    df.withWatermark("timestamp", "5 minutes")
    .groupBy(df["location"])
    .count()
    .orderBy(col("count").desc())
)

null_counts.writeStream.outputMode("complete").option("truncate", False).format("console").start()
query = windowed_counts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
