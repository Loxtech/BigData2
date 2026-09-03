import os
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"
CHECKPOINT_DIR = "/user/hadoop/checkpoints/iris"

original_filename = os.path.basename(urllib.parse.urlparse(URL).path)
transformed_filename = f"transformed_{original_filename}"

schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("species", StringType(), True)
])

def process_batch(df, batch_id):
    """Denne funktion køres automatisk HVER GANG en ny batch opdages."""
    if df.count() > 0:
        print(f"\n Ny batch opdaget (Batch ID: {batch_id})!")
        print(f" Sorterer {df.count()} rækker med Iris-setosa og gemmer i HDFS...")
        
        output_path = os.path.join(HDFS_OUTPUT_DIR, transformed_filename)
        df.write \
          .mode("append") \
          .option("header", "true") \
          .csv(output_path)
        
        print(f" Batch {batch_id} er behandlet og gemt i {output_path}!\n")

def start_realtime_pipeline():
    spark = SparkSession.builder \
        .appName("Flora_RealTime_Pipeline") \
        .getOrCreate()

    # Dæmp interne Spark-advarsler så terminalen holdes ren
    spark.sparkContext.setLogLevel("ERROR")

    print(f" Overvåger HDFS-mappen '{HDFS_INPUT_DIR}' for nye filer... (Tryk Ctrl+C for at stoppe)")

    streaming_df = spark.readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv(HDFS_INPUT_DIR)

    # Transform: Filtrer kun Iris-setosa
    filtered_df = streaming_df.filter(streaming_df["species"] == "Iris-setosa")

    # Load via foreachBatch
    query = filtered_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n Slukker for real-time pipeline...")
        query.stop()

if __name__ == "__main__":
    start_realtime_pipeline()