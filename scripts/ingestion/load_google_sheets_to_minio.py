import json
import os
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql import SparkSession

# ==================== KONFIGURASI PATH & S3A ====================
SPREADSHEET_ID = '10DA2UJ7a-lEyCVqK2tVSZQXWhIizUKewSBJsnfX8tgA'
STATE_FILE = '/opt/airflow/scripts/ingestion/checkpoint_sheets.json'
CREDENTIALS_FILE = '/opt/airflow/scripts/ingestion/credentials.json'

PATH_UNDANGAN = "s3a://etl-data/data-lake/raw/resepsi/undangan/"
PATH_TAMU = "s3a://etl-data/data-lake/raw/resepsi/tamu/"
# ================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {"Undangan": 0, "Tamu": 0}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def run_ingestion():
    # 1. Inisialisasi SparkSession (Bersih dari spasi/karakter liar setelah backslash)
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Resepsi_Incremental_to_MinIO") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    # 2. Konfigurasi Hadoop untuk MinIO
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minio")
    hadoop_conf.set("fs.s3a.secret.key", "minio123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "10")
    hadoop_conf.set("fs.s3a.multipart.size", "104857600")
    hadoop_conf.set("fs.s3a.committer.name", "directory")
    hadoop_conf.set("fs.s3a.committer.staging.tmpdn", "/tmp/spark_staging")
    hadoop_conf.set("fs.s3a.buffer.dir", "/tmp/spark_s3a_buffer")

    # 3. Otentikasi Google Sheets API
    print("[*] Mengautentikasi ke Google API...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        print("[+] Berhasil terhubung ke Google Sheets!")
    except Exception as e:
        print(f"[-] Gagal otentikasi Google Sheets: {e}")
        spark.stop()
        return

    # Load data log row terakhir
    state = load_state()

    # ==============================================================================
    # 4. PROSES SINKRONISASI INCREMENTAL
    # ==============================================================================
    sheets_to_process = [
        {"sheet_name": "Undangan", "path_minio": PATH_UNDANGAN},
        {"sheet_name": "Tamu", "path_minio": PATH_TAMU}
    ]

    for item in sheets_to_process:
        s_name = item["sheet_name"]
        p_minio = item["path_minio"]
        
        print(f"\n====== Memulai Ingestion Incremental Sheet: {s_name} ======")
        try:
            sheet = spreadsheet.worksheet(s_name)
            # Mengambil data dengan header di baris ke-3
            records = sheet.get_all_records(head=3) 
            
            last_processed_row = state.get(s_name, 0)
            total_rows = len(records)
            
            if total_rows > last_processed_row:
                print(f"[+] Menemukan data baru! Memproses baris ke-{last_processed_row + 1} sampai {total_rows}")
                
                # Slicing hanya mengambil data yang belum pernah di-ingest
                new_records = records[last_processed_row:total_rows]
                
                # Konversi list dict -> Pandas DataFrame
                pdf_new = pd.DataFrame(new_records)
                
                # 🛠️ PERBAIKAN UTAMA: Bersihkan nilai kosong/NaN dan paksa skema string 
                # agar Spark tidak mengalami silent error saat infer data
                pdf_new = pdf_new.fillna("")
                pdf_new = pdf_new.astype(str)
                
                # Konversi Pandas ke PySpark DataFrame secara aman
                df_new = spark.createDataFrame(pdf_new)
                
                # Tulis data baru ke MinIO (Append)
                print(f"[-] Menulis {len(new_records)} baris data baru ke MinIO...")
                df_new.write \
                    .mode("append") \
                    .json(p_minio)
                
                # Update state log baris terakhir jika penulisan berhasil
                state[s_name] = total_rows
                save_state(state)
                print(f"[+] Sukses menyimpan data baru '{s_name}' ke MinIO.")
            else:
                print(f"[~] Tidak ada data baru pada sheet '{s_name}' (Data terakhir: {total_rows} baris).")
                
        except Exception as e:
            print(f"[-] Gagal memproses sheet {s_name}: {str(e)}")
            # Raise exception agar Airflow tahu kalau ada langkah yang gagal
            raise e

    # 5. CLOSING
    print("\n[*] Semua proses selesai. Menghentikan Spark Session...")
    spark.stop()

if __name__ == "__main__":
    run_ingestion()