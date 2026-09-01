import os
from pyspark.sql import DataFrame

def load_data_to_hdfs(df: DataFrame, original_url: str, hdfs_output_dir: str):
    """
    Gemmer transformeret DataFrame direkte på HDFS som CSV.
    Navn genereres dynamisk: transformed_<original_filename>
    Overskriver eksisterende data (.mode("overwrite")).
    """
    original_filename = os.path.basename(original_url)
    new_filename = f"transformed_{original_filename}"
    hdfs_output_path = os.path.join(hdfs_output_dir, new_filename)

    # Gem direkte til HDFS i CSV format med headers
    df.coalesce(1).write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(hdfs_output_path)

    print(f"Data gemt succesfuldt på HDFS: {hdfs_output_path}")