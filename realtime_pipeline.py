import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
CHECKPOINT_DIR = "/user/hadoop/checkpoints/iris_hive"
HIVE_DB = "flora_dw"
HIVE_TABLE = "iris_setosa_stream"
FULL_TABLE_NAME = f"{HIVE_DB}.{HIVE_TABLE}"

schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("species", StringType(), True)
])

def process_batch(df, batch_id):
    count = df.count()
    if count > 0:
        print(f"\n[Batch {batch_id}] Modtog {count} rækker. Gemmer i Hive Data Warehouse...")
        
        # Gemmer dataene som Parquet direkte på HDFS i Hive's warehouse mappen
        hive_target_path = f"hdfs://localhost:9000/user/hive/warehouse/{HIVE_DB}.db/{HIVE_TABLE}"
        
        df.write \
          .mode("append") \
          .format("parquet") \
          .save(hive_target_path)
          
        print(f"[Batch {batch_id}] Succes! Data gemt på HDFS til Hive: {hive_target_path}\n")

def start_realtime_pipeline():
    # Standard PySpark Session - UDEN enableHiveSupport for at undgå klasselæsningsfejl
    spark = SparkSession.builder \
        .appName("Flora_Hive_RealTime_Pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"Overvåger HDFS '{HDFS_INPUT_DIR}' og klargør streaming til '{HIVE_DB}'...")

    streaming_df = spark.readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv(HDFS_INPUT_DIR)

    filtered_df = streaming_df.filter(streaming_df["species"] == "Iris-setosa")

    query = filtered_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    try:
        query.awaitTermination()
    except (KeyboardInterrupt, Exception):
        print("\nSlukker for pipeline...")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    start_realtime_pipeline()