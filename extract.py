import os
import sys
import subprocess
import urllib.parse
import requests

URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
HDFS_INPUT_DIR = "/user/hadoop/Input_dir"
HEADER = "sepal_length,sepal_width,petal_length,petal_width,species\n"

def get_filename_from_url(url):
    """Udlæser filnavnet automatisk fra URL'en uden hardcoding."""
    parsed_url = urllib.parse.urlparse(url)
    return os.path.basename(parsed_url.path)

def extract_data(url=URL):
    filename = get_filename_from_url(url)
    hdfs_file_path = f"{HDFS_INPUT_DIR}/{filename}"

    # -------------------------------------------------------------------------
    # DOKUMENTATION TIL OPGAVEN Requests + HDFS:
    #
    # 1. Datasikkerhed og Command-Line Injection:
    #    Ved brug af 'requests' styres HTTP-kaldet fuldstændigt af Python-koden, 
    #    hvilket udelukker shell-injection. Subprocess til HDFS køres med en 
    #    liste af argumenter uden shell=True, hvilket forhindrer command injection.
    #
    # 2. Dataintegritet og Robusthed:
    #    - 'stream=True' og 'timeout=10' sikrer, at store filer hentes i mindre bidder,
    #      og at programmet ikke hænger uendeligt ved netværksfejl.
    #    - 'response.raise_for_status()' afbryder oprettelsen med det samme, hvis
    #      serveren returnerer en fejl (f.eks. 404 eller 500).
    # -------------------------------------------------------------------------

    print(f"Henter data med requests til HDFS: {hdfs_file_path}...")

    # Subprocess kommando til at skrive direkte til HDFS fra stdin ('-')
    hdfs_cmd = ["hdfs", "dfs", "-put", "-f", "-", hdfs_file_path]

    try:
        # Start HDFS-processen og åbn op for stdin stream
        hdfs_process = subprocess.Popen(hdfs_cmd, stdin=subprocess.PIPE)

        # Hent data fra webserveren som en stream
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()  # Tjekker for HTTP-fejl (404, 500 osv.)

        # 1. Skriv overskriften (header) først
        hdfs_process.stdin.write(HEADER.encode('utf-8'))

        # 2. Stream data i bidder (chunks) fra requests direkte over i HDFS stdin
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                hdfs_process.stdin.write(chunk)

        # Luk pipe og vent på at HDFS afslutter overførslen
        hdfs_process.stdin.close()
        hdfs_process.wait()

        if hdfs_process.returncode == 0:
            print("Extract fuldført! Data blev gemt i HDFS uden at røre den lokale disk.")
        else:
            print(f"Fejl under skrivning til HDFS. Fejlkode: {hdfs_process.returncode}")

    except requests.exceptions.RequestException as e:
        print(f"Netværksfejl under hentning med requests: {e}")
    except Exception as e:
        print(f"Uventet fejl: {e}")

if __name__ == "__main__":
    extract_data()