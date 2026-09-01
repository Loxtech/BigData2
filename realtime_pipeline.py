import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"
CHECKPOINT_DIR = "/user/hadoop/checkpoints/iris"

# Definer skema på forhånd til Streaming
schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("species", StringType(), True)
])

def start_realtime_stream():
    spark = SparkSession.builder \
        .appName("Flora_RealTime_Streaming") \
        .getOrCreate()

    print(f"Overvåger HDFS mappe for nye filer: {HDFS_INPUT_DIR}...")

    # 1. Stream fra HDFS Input Mappe
    streaming_df = spark.readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv(HDFS_INPUT_DIR)

    # 2. Real-time Transform
    filtered_stream = streaming_df.filter(streaming_df["species"] == "Iris-setosa")

    # 3. Real-time Load (WriteStream til HDFS Output)
    query = filtered_stream.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("header", "true") \
        .option("path", os.path.join(HDFS_OUTPUT_DIR, "realtime_transformed_iris")) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_realtime_stream()