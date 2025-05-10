from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

spark = SparkSession.builder \
    .appName("YoutubeVideoStreaming") \
    .config("spark.jars.packages",
             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0, org.apache.hadoop:hadoop-common:3.3.6, org.apache.hadoop:hadoop-hdfs:3.3.6") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode1:9000") \
    .getOrCreate()

schema = StructType([
    StructField("channel_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("published_at", TimestampType(), True),
    StructField("view_count", StringType(), True),
    StructField("like_count", StringType(), True),
    StructField("comment_count", StringType(), True),
    StructField("date", TimestampType(), True)
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "youtube_video_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_selected = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col('json_data'), schema).alias('data')) \
    .select("data.*") 

today = datetime.now()

df_selected = df_selected.withColumn("year", lit(today.year)) \
    .withColumn("month", lit(today.month)) \
    .withColumn("day", lit(today.day))

df_selected = df_selected.withColumn("view_count", col("view_count").cast("int")) \
       .withColumn("like_count", col("like_count").cast("int")) \
       .withColumn("comment_count", col("comment_count").cast("int"))

query = df_selected.writeStream \
    .partitionBy("year", "month", "day") \
    .format("parquet") \
    .option("path", "hdfs://namenode1:9000/youtube_DE_project/datalake/youtube_video/raw/") \
    .option("checkpointLocation", "hdfs://namenode1:9000/youtube_DE_project/datalake/youtube_video/raw_checkpoint/") \
    .outputMode("append") \
    .trigger(once=True) \
    .start()

query.awaitTermination()