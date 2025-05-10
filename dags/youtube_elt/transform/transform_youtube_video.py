from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("YoutubeVideoTransformToHive") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages",
             "org.apache.spark:spark-hive_2.12:3.4.0, org.postgresql:postgresql:42.6.0, org.apache.hadoop:hadoop-common:3.3.6, org.apache.hadoop:hadoop-hdfs:3.3.6") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode1:9000") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode1:9000/youtube_DE_project/datawarehouse/") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

try:
    spark.sql("CREATE DATABASE IF NOT EXISTS youtube_project_analytics")
    spark.sql("USE youtube_project_analytics")
    
    df = spark.read.parquet("hdfs://namenode1:9000/youtube_DE_project/datalake/youtube_video/raw/") 

    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day

    df = df.filter((df["year"] == year) & (df["month"] == month) & (df["day"] == day))

    df.write.mode("append") \
      .partitionBy("year", "month", "day") \
      .format("parquet") \
      .option("path", "hdfs://namenode1:9000/youtube_DE_project/datawarehouse/youtube_project_analytics/video_data_analytics") \
      .saveAsTable("youtube_project_analytics.video_data_analytics")
    
    print("Transform và load dữ liệu thành công!")
    
except Exception as e:
    print(f"Lỗi khi xử lý dữ liệu: {str(e)}")
    raise e
finally:
    spark.stop()