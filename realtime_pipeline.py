import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.functions import udf

# Importer AES-GCM krypteringen fra vores nye sikkerhedsmodul
from security import encrypt_dataset_gcm

HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
CHECKPOINT_DIR = "/user/hadoop/checkpoints/iris_hive"
HIVE_DB = "flora_dw"
HIVE_TABLE = "iris_setosa_stream"
FULL_TABLE_NAME = f"{HIVE_DB}.{HIVE_TABLE}"

# Schema for indkommende streaming-data
schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("species", StringType(), True)
])

# Spark UDF der krypterer en strengværdi til AES-GCM hex-format
@udf(returnType=StringType())
def encrypt_species_udf(species_name):
    if species_name is None:
        return None
    # Krypterer teksten og returnerer som læsbar Hex-streng til Hive/Parquet
    return encrypt_dataset_gcm(str(species_name)).hex()

def process_batch(df, batch_id):
    count = df.count()
    if count > 0:
        print(f"\n[Batch {batch_id}] Modtog {count} rækker. Krypterer data med AES-GCM (Krav 3)...")
        
        # Anvend AES-GCM kryptering på species-kolonnen
        encrypted_df = df.withColumn("species", encrypt_species_udf(df["species"]))
        
        hive_target_path = f"hdfs://localhost:9000/user/hive/warehouse/{HIVE_DB}.db/{HIVE_TABLE}"
        
        # Gemmer de krypterede data som Parquet i Hive warehouse-mappen
        encrypted_df.write \
            .mode("append") \
            .format("parquet") \
            .save(hive_target_path)
          
        print(f"[Batch {batch_id}] Succes! Krypteret data gemt i Hive: {hive_target_path}\n")

def start_realtime_pipeline():
    spark = SparkSession.builder \
        .appName("Flora_Hive_RealTime_Pipeline_Encrypted") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"Overvåger HDFS '{HDFS_INPUT_DIR}' og klargør streaming til '{HIVE_DB}' (Med AES-GCM kryptering)...")

    streaming_df = spark.readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv(HDFS_INPUT_DIR)

    # Filtrer observationer
    filtered_df = streaming_df.filter(streaming_df["species"] == "Iris-setosa")

    # Start stream med foreachBatch
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