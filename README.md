# Realtime Data Pipeline (Krav 2)

Dette projekt demonstrerer en automatisk realtime datastrøm (streaming pipeline) ved hjælp af Hadoop (HDFS) og Apache Spark (Structured Streaming) i Python.

## Koncept & arkitektur

1. **Extract** (`extract.py`): Henter rå data fra et eksternt API/URL og streamer det direkte ind i HDFS-inputmappen (`/user/hadoop/Input_dir`) uden at bruge lokal diskplads. Hvert udtræk tildeles et unikt tidsstempel i filnavnet.
2. **Stream Processing** (`realtime_pipeline.py`): En PySpark-applikation overvåger HDFS-inputmappen i realtid. Når en ny fil ankommer, behandles data i et micro-batch, filtreres for `Iris-setosa`, og gemmes direkte i HDFS-outputmappen (`/user/hadoop/Output_dir`).

## Projektfiler

```
├── extract.py              # Henter data fra API og skubber direkte til HDFS
├── realtime_pipeline.py    # Overvåger HDFS og udfører streaming transformation
└── README.md               # Projektdokumentation
```

## Kørselsvejledning (step-by-step)

### Step 1: Start Hadoop Cluster

Sørg for, at HDFS er aktivt i din terminal:

```bash
start-dfs.sh
```

### Step 2: Aktiver det virtuelle miljø

```bash
cd ~/BigData2
source .venv/bin/activate
```

### Step 3: Start realtime streaming pipeline (Terminal 1)

Start Spark streaming-pipelinen. Den vil stå i en aktiv lytte-tilstand og vente på nye data i HDFS:

```bash
python realtime_pipeline.py
```

Forventet output:

```
Overvåger HDFS-mappen '/user/hadoop/Input_dir' for nye filer... (Tryk Ctrl+C for at stoppe)
```

### Step 4: Udløs datastrøm via Extract (Terminal 2)

Åbn en ny terminalfane, aktiver miljøet og kør `extract.py` for at hente data fra API'et:

```bash
cd ~/BigData2
source .venv/bin/activate
python extract.py
```

Forventet output:

```
Henter data med requests til HDFS: /user/hadoop/Input_dir/iris_20260904_084500.csv...
Extract fuldført! Data blev gemt i HDFS uden at røre den lokale disk.
```

## Demonstration & verifikation

### 1. Reaktion i PySpark (Terminal 1)

Så snart `extract.py` udføres, opdager PySpark den nye fil og udskriver batch-status i konsollen:

```
Ny batch opdaget (Batch ID: 0)!
Sorterer 50 rækker med Iris-setosa og gemmer i HDFS...
Batch 0 er behandlet og gemt i /user/hadoop/Output_dir/transformed_iris.csv!
```

### 2. Verifikation i HDFS (Terminal 2)

Du kan kontrollere, at filerne reelt er oprettet og skrevet til HDFS med følgende kommandoer:

```bash
# Se rådataen modtaget fra API'et
hdfs dfs -ls /user/hadoop/Input_dir/

# Se de filtrerede/behandlede data genereret af Spark
hdfs dfs -ls /user/hadoop/Output_dir/transformed_iris.csv

# Vis de første rækker af det filtrerede resultat
hdfs dfs -cat /user/hadoop/Output_dir/transformed_iris.csv/*.csv | head -n 10
```

## Nulstilling af pipeline (test fra starten)

Hvis du ønsker at nulstille pipelinen og starte fra et helt rent bord, skal du slette checkpointet og outputmappen i HDFS:

```bash
hdfs dfs -rm -r -f /user/hadoop/checkpoints/iris
hdfs dfs -rm -r -f /user/hadoop/Output_dir/
hdfs dfs -rm -r -f /user/hadoop/Input_dir/*
```