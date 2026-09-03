# Big Data Pipeline, Encryption & Visualization (Krav 1, Krav 2 & Krav 3)

Dette projekt demonstrerer en end-to-end Big Data arkitektur på WSL/Hadoop. Systemet streamer realtiddata med PySpark, krypterer følsomme felter med AES-GCM (Krav 3), gemmer det som komprimeret Parquet i et Hive Data Warehouse på HDFS, og tilbyder et dedikeret visualiseringsmodul til udtræk og grafer.

---

## Systemarkitektur & Komponenter

* **Hadoop HDFS & YARN** (v3.5) – Distribueret lagring og ressourcestyring
* **Hive Data Warehouse** (v4.2) – Struktureret datalagring med MySQL Metastore
* **PySpark** (v4.2) – Structured Streaming og datatransformation
* **AES-GCM (Cryptography)** – Sikkerhedsmodul til Data at Rest kryptering
* **Matplotlib & Seaborn** – Generering af analytiske diagrammer
* **Java 17** – Runtime miljø for Spark og Hadoop

---

## Opstart af Tjenester (Efter Genstart)

Kør følgende kommandoer i din WSL-terminal, når systemet har været slukket:

### 1. Start Hadoop & MySQL
start-dfs.sh
start-yarn.sh
sudo service mysql start

### 2. Start Hive Services (i hver sin terminal-fane)
* **Metastore:**
  hive --service metastore

* **HiveServer2 (vent ~15 sek. før brug):**
  hiveserver2

---

## Krav 1 & Krav 2: Real-time ETL Streaming med AES-GCM Kryptering

Pipelinen overvåger HDFS-mappen `/user/hadoop/Input_dir`, filtrerer på `Iris-setosa`, krypterer kolonnen `species` ved hjælp af AES-GCM via PySpark UDFs (Krav 3) og gemmer data som Parquet direkte i Hive Data Warehouse.

### Terminal 1: Kør streaming-lytteren
cd ~/BigData2
source .venv/bin/activate
python realtime_pipeline.py

*(Lad denne terminal stå åben til at overvåge indkommende filer)*

### Terminal 2: Kør oprydning og udløs workflow
Åbn en ny terminal-fane:

# 1. (Valgfrit) Nulstil HDFS og databasen for en ren kørsel
python deleteData.py

# 2. Udløs Extract fra web og kør visualiserings-trigget
python main.py

---

## Verificer Kryptering i Hive (Beeline)

Åbn Beeline for at verificere, at dataene reelt er krypteret i databasen (*Data at Rest*):

beeline -u jdbc:hive2://localhost:10000

Kør følgende SQL i Beeline:
SHOW DATABASES;
USE flora_dw;
SHOW TABLES;
SELECT * FROM iris_setosa_stream LIMIT 10;

*Forventet resultat:* Kolonnen `species` fremstår som en hexadecimal ciffertekst (f.eks. `a3f89021e...`) i stedet for klartekst.

---

## Krav 3: Visualiseringsmodul & HDFS Eksport

Visualiseringsmodulet (`visualization.py`) læser de krypterede Parquet-data ud fra Hive på HDFS, dekrypterer `species`-kolonnen i hukommelsen via Pandas (`p_df`), genererer tre analytiske grafer og uploader dem direkte til `/user/hadoop/Output_dir` på HDFS.

### 1. Bekræft at filerne er oprettet på HDFS:
hdfs dfs -ls /user/hadoop/Output_dir

### 2. Vis diagrammerne i Windows:
Diagrammerne hentes automatisk ned i mappen `Diagrammer/` via `main.py` (eller `fetch_charts.py`). 

---

## Filoversigt

* `main.py` - Hovedorchestator (Extract, trigger for visualisering og fetch)
* `realtime_pipeline.py` - PySpark Structured Streaming pipeline med AES-GCM kryptering
* `security.py` - Sikkerhedsmodul med AES-GCM og AES-CBC krypteringsmetoder (Krav 3)
* `deleteData.py` - Oprydningsscript til nulstilling af HDFS-mapper og datalagre
* `visualization.py` - Visualiseringsmodul (Scatter-plot, histogram og boxplot med dekryptering via `p_df`)
* `fetch_charts.py` - Automatiseret udtræk af HDFS .png-filer til lokal mappe
* `Diagrammer/` - Lokal mappe med de genererede .png visualiseringer