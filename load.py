import os
import subprocess
from security import encrypt_dataset_gcm

def save_encrypted_csv_to_hdfs(df, hdfs_output_path: str):
    """
    Konverterer PySpark DataFrame til CSV-streng, krypterer den med AES-GCM,
    og gemmer den krypterede binære fil direkte på HDFS.
    """
    # 1. Konverter DataFrame til en samlet CSV-streng
    pandas_df = df.toPandas()
    raw_csv_data = pandas_df.to_csv(index=False)

    # 2. Krypter dataene med AES-GCM
    encrypted_bytes = encrypt_dataset_gcm(raw_csv_data)

    # 3. Gem den krypterede binære fil midlertidigt og upload til HDFS
    local_temp_file = "/tmp/encrypted_iris.enc"
    with open(local_temp_file, "wb") as f:
        f.write(encrypted_bytes)

    # Upload krypteret fil til HDFS
    os.system(f"hdfs dfs -put -f {local_temp_file} {hdfs_output_path}")
    os.remove(local_temp_file)
    print(f"[KRAV 3] Krypteret data (AES-GCM) gemt succesfuldt på HDFS: {hdfs_output_path}")