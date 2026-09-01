from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str = "Flora_Batch_ETL") -> SparkSession:
    """Opretter en SparkSession mod Hadoop HDFS."""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def transform_iris_data(spark: SparkSession, hdfs_input_path: str) -> DataFrame:
    """
    Læser CSV fra HDFS, filtrerer rækker hvor species == 'Iris-setosa'.
    """
    # Læs direkte fra HDFS med automatisk schema-inferering
    df = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

    # Filtrer observationer
    filtered_df = df.filter(df["species"] == "Iris-setosa")

    return filtered_df