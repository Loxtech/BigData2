from extract import extract_data_to_hdfs
from transform import get_spark_session, transform_iris_data
from load import load_data_to_hdfs

# Konfiguration
DATA_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
HEADER_LINE = "sepal_length,sepal_width,petal_length,petal_width,species"
HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"

def main():
    print("--- 1. EXTRACT (Streaming til HDFS) ---")
    hdfs_input_path = extract_data_to_hdfs(DATA_URL, HDFS_INPUT_DIR, HEADER_LINE)

    print("--- 2. TRANSFORM (PySpark på HDFS) ---")
    spark = get_spark_session()
    transformed_df = transform_iris_data(spark, hdfs_input_path)

    print("--- 3. LOAD (Skriver til HDFS) ---")
    load_data_to_hdfs(transformed_df, DATA_URL, HDFS_OUTPUT_DIR)

    spark.stop()
    print("Batch ETL gennemført!")

if __name__ == "__main__":
    main()