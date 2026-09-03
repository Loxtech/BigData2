import os

HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"
HIVE_DATA_PATH = "/user/hive/warehouse/flora_dw.db/iris_setosa_stream"
CHECKPOINT_DIR = "/user/hadoop/checkpoints/iris_hive"

def clean_all():
    print("--- NULSTILLER OG RYDDER OP I DATA ---")
    
    # 1. Slet HDFS streaming checkpoint & Hive Parquet data
    os.system(f"hdfs dfs -rm -r -f {CHECKPOINT_DIR}")
    os.system(f"hdfs dfs -rm -r -f {HIVE_DATA_PATH}")
    
    # 2. Tøm Input og Output mapper på HDFS
    os.system(f"hdfs dfs -rm -r -f {HDFS_INPUT_DIR}/*")
    os.system(f"hdfs dfs -rm -r -f {HDFS_OUTPUT_DIR}/*")
    
    # 3. Genopret HDFS mapperne
    os.system(f"hdfs dfs -mkdir -p {HDFS_INPUT_DIR}")
    os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT_DIR}")
    
    # 4. Slet den lokale Diagrammer-mappe på WSL
    os.system("rm -rf ~/BigData2/Diagrammer")
    
    print("[SUCCESS] Alt data er slettet på HDFS og lokalt! Systemet er klar til ren kørsel.\n")

if __name__ == "__main__":
    clean_all()