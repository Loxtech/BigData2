import os
import subprocess
import requests

def extract_data_to_hdfs(data_url: str, hdfs_input_dir: str, header_line: str) -> str:
    """
    Sikkerhed & Integritet:
    - Command Injection: Anvender subprocess.Popen med en liste (shell=False), hvilket 
      forhindrer angribere i at køre tilfældige shell-kommandoer via URL'en.
    - HTTPS Transport: requests validerer SSL/TLS certifikater automatisk.
    
    Robusthed & Streaming:
    - Anvender stream=True og iter_content(chunk_size=1024) så store filer hentes i små 
      blokke uden at fylde RAM eller berøre det lokale disk-filsystem (LFS).
    
    HDFS Integration:
    - Piber data direkte ind i HDFS-CLI ('hdfs dfs -put - <hdfs_path>') via stdin.
    """
    # Udfind filnavn dynamisk fra URL (ingen hardcoding)
    filename = os.path.basename(data_url)
    hdfs_filepath = os.path.join(hdfs_input_dir, filename)

    # 1. Sikre at HDFS mappen eksisterer
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_input_dir], check=True)

    # 2. Start HDFS process der modtager data via stdin og overskriver eksisterende (-f)
    hdfs_cmd = ["hdfs", "dfs", "-put", "-f", "-", hdfs_filepath]
    process = subprocess.Popen(hdfs_cmd, stdin=subprocess.PIPE)

    # 3. Stream fra HTTPS og skriv direkte til HDFS stdin
    response = requests.get(data_url, stream=True)
    response.raise_for_status()

    # Skriv header først
    process.stdin.write(f"{header_line}\n".encode("utf-8"))

    # Skriv indhold i chunks
    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            process.stdin.write(chunk)

    process.stdin.close()
    process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"Fejl ved overførsel til HDFS: {hdfs_filepath}")

    return hdfs_filepath