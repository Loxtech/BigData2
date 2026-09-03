import time
from extract import extract_data, URL, HDFS_INPUT_DIR
from visualization import generate_scatterplot, generate_histogram, generate_boxplot, HDFS_OUTPUT_DIR
from fetch_charts import fetch_images
from pyspark.sql import SparkSession

HIVE_DATA_PATH = "hdfs://localhost:9000/user/hive/warehouse/flora_dw.db/iris_setosa_stream"

def main():
    # 1. Extract: Hent data direkte fra nettet til HDFS input-mappe
    print("\n--- 1. EXTRACT (Henter data via HTTP til HDFS) ---")
    extract_data(URL)
    
    print("\n Vent da venligst ca. 5-10 sekunder på, at realtime_pipeline.py opdager den nye fil...")
    time.sleep(8)  # Giver PySpark Streaming tid til at opfange filen og skrive til Hive

    # 2. Visualisering: Læs Parquet direkte fra Hive Warehouse
    print("\n--- 2. VISUALISERING (Læser fra Hive & genererer grafer) ---")
    spark = SparkSession.builder \
        .appName("Flora_Visualization_Main") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        hive_df = spark.read.parquet(HIVE_DATA_PATH)
        print(f"Hentede {hive_df.count()} rækker fra Hive. Genererer diagrammer...")
        
        generate_scatterplot(hive_df, HDFS_OUTPUT_DIR)
        generate_histogram(hive_df, HDFS_OUTPUT_DIR)
        generate_boxplot(hive_df, HDFS_OUTPUT_DIR)
    except Exception as e:
        print(f"[FEJL] Kunne ikke læse data fra Hive endnu. Er realtime_pipeline.py startet? Fejl: {e}")
    finally:
        spark.stop()

    # 3. Fetch: Hent diagrammer ned til lokal mappe
    print("\n--- 3. FETCH CHARTS (Eksporterer diagrammer til Windows) ---")
    fetch_images()

    print("\n [SUCCESS] Arbejdsgangen er gennemført!")

if __name__ == "__main__":
    main()