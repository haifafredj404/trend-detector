from pyspark.sql import SparkSession
import os

# 🔥 Désactiver Hadoop sur Windows
os.environ["HADOOP_HOME"] = ""
os.environ["hadoop.home.dir"] = ""

spark = SparkSession.builder \
    .appName("TestSparkNoHadoop") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
    .getOrCreate()

print("✅ Spark démarre SANS Hadoop")

spark.stop()

