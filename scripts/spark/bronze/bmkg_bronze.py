from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# 1. Inisialisasi Spark Session (Murni untuk AWS S3/MinIO, Tanpa Driver Kafka)
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Batch-to-Bronze-BMKG-SQL") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==================== KONFIGURASI PATH ====================
# Jalur tempat script ingestion (Airflow) menaruh data mentah JSON dari API BMKG
INPUT_RAW_JSON_PATH = "s3a://etl-data/data-lake/raw/bmkg/*.json" 

# Jalur target untuk menyimpan hasil ke Bronze Layer
OUTPUT_BRONZE_PATH = "s3a://etl-data/data-lake/bronze/bmkg/"
# ==========================================================

print(f"[*] Membaca data batch JSON mentah dari: {INPUT_RAW_JSON_PATH}")

try:
    # 2. Membaca Data secara BATCH (Menggunakan spark.read, bukan readStream)
    # Kita langsung pasang skema DDL SQL agar Spark tidak lambat mengecek (infer) schema JSON raksasa
    json_ddl_schema = (
        "lokasi STRUCT<provinsi:STRING,kotkab:STRING,kecamatan:STRING,desa:STRING,lon:STRING,lat:STRING,timezone:STRING>,"
        "data ARRAY<STRUCT<cuaca:ARRAY<ARRAY<STRUCT<datetime:STRING,local_datetime:STRING,t:STRING,weather_desc:STRING,hu:STRING,ws:STRING,tp:STRING>>>>>"
    )
    
    df_raw = spark.read \
        .schema(json_ddl_schema) \
        .json(INPUT_RAW_JSON_PATH)

    # 3. Daftarkan ke Temporary View agar bisa dieksekusi dengan Spark SQL
    df_raw.createOrReplaceTempView("raw_batch_bmkg")

    # 4. Eksekusi Logika Flattening Data menggunakan Spark SQL
    # 💡 KOREKSI: query dibersihkan dari fungsi dari_json/CAST biner karena format input sudah murni file JSON asli
    df_bronze = spark.sql("""
        WITH flattened_lokasi AS (        
            SELECT 
                lokasi.provinsi AS provinsi,
                lokasi.kotkab AS kotkab,
                lokasi.kecamatan AS kecamatan,
                lokasi.desa AS desa,
                lokasi.lon AS lon,
                lokasi.lat AS lat,
                lokasi.timezone AS timezone,
                data AS array_data
            FROM raw_batch_bmkg
        )
        SELECT 
            CURRENT_TIMESTAMP() AS ingested_at,
            f.provinsi,
            f.kotkab,
            f.kecamatan,
            f.desa,
            f.lon,
            f.lat,
            f.timezone,                
            tiap_waktu.datetime,
            tiap_waktu.local_datetime,
            tiap_waktu.t,
            tiap_waktu.weather_desc,
            tiap_waktu.hu,
            tiap_waktu.ws,
            tiap_waktu.tp 
        FROM flattened_lokasi f
        LATERAL VIEW explode(f.array_data) t1 AS grup_data
        LATERAL VIEW explode(grup_data.cuaca) t2 AS list_cuaca
        LATERAL VIEW explode(list_cuaca) t3 AS tiap_waktu
    """)

    # 5. Simpan Hasil ke MinIO (Format Parquet, Mode Append)
    print(f"[-] Menulis data hasil flattening ke Bronze MinIO: {OUTPUT_BRONZE_PATH}")
    
    df_bronze.write \
        .mode("append") \
        .format("parquet") \
        .save(OUTPUT_BRONZE_PATH)
        
    print("[+] Ingestion Batch Bronze BMKG Sukses Selesai!")

except Exception as e:
    print(f"[-] Terjadi kegagalan proses: {str(e)}")
    raise e

finally:
    # 6. Menutup Spark Session secara bersih
    print("[*] Menghentikan Spark Session...")
    spark.stop()