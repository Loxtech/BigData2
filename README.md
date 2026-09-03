# Big Data Pipeline & Visualization (Krav 1 & Krav 2)

Dette projekt demonstrerer en end-to-end Big Data arkitektur på WSL/Hadoop. Systemet streamer realtiddata med PySpark, gemmer det som komprimeret Parquet i et Hive Data Warehouse på HDFS, og tilbyder et dedikeret visualiseringsmodul til udtræk og grafer.

---

## Systemarkitektur & Komponenter

* **Hadoop HDFS & YARN** (v3.5) – Distribueret lagring og ressourcestyring
* **Hive Data Warehouse** (v4.2) – Struktureret datalagring med MySQL Metastore
* **PySpark** (v4.2) – Structured Streaming og datatransformation
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

## Krav 1: Real-time ETL Streaming Pipeline

Pipelinen overvåger HDFS-mappen `/user/hadoop/Input_dir`, filtrerer på mønsteret `Iris-setosa` og gemmer data som Parquet direkte i Hive Data Warehouse.

### Kør pipelinen:
cd ~/BigData2
source .venv/bin/activate
python realtime_pipeline.py

### Verificer i Hive (Beeline):
Åbn en ny terminal for at inspicere den oprettede database og tabel:
beeline -u jdbc:hive2://localhost:10000

Kør følgende SQL i Beeline:
SHOW DATABASES;
USE flora_dw;
SHOW TABLES;
SELECT * FROM iris_setosa_stream LIMIT 10;

---

## Krav 2: Visualiseringsmodul & HDFS Eksport

Visualiseringsmodulet (`visualization.py`) læser data ud fra Hive på HDFS, genererer tre analytiske grafer og uploader dem direkte til `/user/hadoop/Output_dir` på HDFS.

### 1. Generer diagrammerne:
python visualization.py

### 2. Bekræft at filerne er oprettet på HDFS:
hdfs dfs -ls /user/hadoop/Output_dir

### 3. Hent diagrammerne til lokal mappen og vis i Windows:
Kør hjælpescriptet for at hente billederne ned i mappen `Diagrammer/`:
python fetch_charts.py

Åbn mappen direkte i Windows Stifinder:
explorer.exe Diagrammer

---

## Filoversigt

* `realtime_pipeline.py` - PySpark Structured Streaming pipeline til HDFS/Hive
* `visualization.py` - Modul med funktioner til scatter-plot, histogram og boxplot
* `fetch_charts.py` - Automatiseret udtræk af HDFS .png-filer til lokal mappe
* `Diagrammer/` - Lokal mappe med de genererede .png visualiseringer