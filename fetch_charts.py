import os

HDFS_OUTPUT_DIR = "/user/hadoop/Output_dir"
# Opretter mappen ~/BigData2/Diagrammer
LOCAL_DEST_DIR = os.path.expanduser("~/BigData2/Diagrammer")

files = [
    "scatter_plot.png",
    "histogram.png",
    "boxplot.png"
]

def fetch_images():
    # 1. Sikr at den lokale mappe eksisterer
    os.makedirs(LOCAL_DEST_DIR, exist_ok=True)
    
    print(f"Henter diagrammer fra HDFS ('{HDFS_OUTPUT_DIR}') til lokalt ('{LOCAL_DEST_DIR}')...\n")
    
    for file_name in files:
        hdfs_path = f"{HDFS_OUTPUT_DIR}/{file_name}"
        local_path = os.path.join(LOCAL_DEST_DIR, file_name)
        
        # 2. Henter filen fra HDFS med hdfs dfs -get -f
        cmd = f"hdfs dfs -get -f {hdfs_path} {local_path}"
        exit_code = os.system(cmd)
        
        if exit_code == 0:
            print(f"[SUCCESS] Hentede {file_name} -> {local_path}")
        else:
            print(f"[FEJL] Kunne ikke hente {file_name} fra HDFS.")

    print(f"\nFærdig! Filerne er gemt i: {LOCAL_DEST_DIR}")

if __name__ == "__main__":
    fetch_images()