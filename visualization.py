import os
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession, DataFrame

# Importer dekryptering fra sikkerhedsmodulet
from security import decrypt_dataset_gcm

HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"
HIVE_DATA_PATH = "hdfs://localhost:9000/user/hive/warehouse/flora_dw.db/iris_setosa_stream"

def decrypt_species_in_pandas(p_df):
    """Hjælpefunktion: Dekrypterer species-kolonnen fra Hex-streng tilbage til klartekst."""
    if 'species' in p_df.columns:
        p_df['species'] = p_df['species'].apply(
            lambda x: decrypt_dataset_gcm(bytes.fromhex(x)) if isinstance(x, str) else x
        )
    return p_df

def generate_scatterplot(df: DataFrame, output_dir: str = HDFS_OUTPUT_DIR):
    """Genererer et scatter-plot over sepal_length vs petal_length og gemmer på HDFS."""
    p_df = df.toPandas()
    p_df = decrypt_species_in_pandas(p_df)  # Dekrypterer species til graferne
    
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=p_df, x="sepal_length", y="petal_length", hue="species", palette="deep")
    plt.title("Scatter Plot: Sepal Length vs Petal Length")
    plt.xlabel("Sepal Length")
    plt.ylabel("Petal Length")
    plt.legend(title="Species")
    
    local_path = "/tmp/scatter_plot.png"
    plt.savefig(local_path)
    plt.close()
    
    os.system(f"hdfs dfs -put -f {local_path} {output_dir}/scatter_plot.png")
    print(f"Scatter-plot gemt på HDFS: {output_dir}/scatter_plot.png")

def generate_histogram(df: DataFrame, output_dir: str = HDFS_OUTPUT_DIR):
    """Genererer et histogram over sepal_width og gemmer på HDFS."""
    p_df = df.toPandas()
    p_df = decrypt_species_in_pandas(p_df)
    
    plt.figure(figsize=(8, 6))
    sns.histplot(data=p_df, x="sepal_width", kde=True, color="skyblue")
    plt.title("Histogram: Distribution of Sepal Width")
    plt.xlabel("Sepal Width")
    plt.ylabel("Frequency")
    
    local_path = "/tmp/histogram.png"
    plt.savefig(local_path)
    plt.close()
    
    os.system(f"hdfs dfs -put -f {local_path} {output_dir}/histogram.png")
    print(f"Histogram gemt på HDFS: {output_dir}/histogram.png")

def generate_boxplot(df: DataFrame, output_dir: str = HDFS_OUTPUT_DIR):
    """Genererer et boxplot af alle numeriske Iris-setosa målinger og gemmer på HDFS."""
    p_df = df.toPandas()
    p_df = decrypt_species_in_pandas(p_df)
    
    numeric_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
    data_to_plot = [p_df[col].dropna() for col in numeric_cols]
    
    plt.figure(figsize=(9, 5))
    bp = plt.boxplot(data_to_plot, tick_labels=numeric_cols, patch_artist=True)
    
    for patch in bp['boxes']:
        patch.set_facecolor('white')
        patch.set_edgecolor('black')
        
    for median in bp['medians']:
        median.set_color('#ff7f0e')
        median.set_linewidth(1.5)

    plt.title("Boxplots af alle numeriske Iris-setosa målinger", fontsize=11)
    plt.ylabel("Værdi", fontsize=10)
    plt.grid(axis='y', linestyle='-', alpha=0.5)
    
    plt.tight_layout()
    
    local_path = "/tmp/boxplot.png"
    plt.savefig(local_path, dpi=300)
    plt.close()
    
    os.system(f"hdfs dfs -put -f {local_path} {output_dir}/boxplot.png")
    print(f"Boxplot gemt på HDFS: {output_dir}/boxplot.png")

if __name__ == "__main__":
    os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT_DIR}")

    spark = SparkSession.builder \
        .appName("Flora_Visualization_Module") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    print("\nLæser krypteret data direkte fra Hive Warehouse på HDFS...")
    df = spark.read.parquet(HIVE_DATA_PATH)
    
    print(f"Hentede {df.count()} rækker. Dekrypterer og genererer diagrammer...")
    
    generate_scatterplot(df)
    generate_histogram(df)
    generate_boxplot(df)
    
    print("\n[SUCCESS] Alle diagrammer er oprettet og gemt i Output_dir på HDFS!\n")
    spark.stop()