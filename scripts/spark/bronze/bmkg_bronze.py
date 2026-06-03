from pyspark.sql import SparkSession

# 1. Inisialisasi Spark Session dengan Driver Kafka & AWS S3
spark = SparkSession.builder \
    .appName("Kafka-to-Bronze-BMKG-SQL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                  "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Membaca Aliran Data (Stream) dari Kafka Topic
df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bmkg_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Daftarkan Stream Kafka menjadi Temporary View agar bisa dieksekusi dengan Spark SQL
df_kafka_stream.createOrReplaceTempView("raw_kafka_bmkg")

# Schema DDL SQL untuk membongkar JSON bertingkat milik BMKG
json_ddl_schema = (
    "lokasi STRUCT<provinsi:STRING,kotkab:STRING,kecamatan:STRING,desa:STRING,lon:STRING,lat:STRING,timezone:STRING>,"
    "data ARRAY<STRUCT<cuaca:ARRAY<ARRAY<STRUCT<datetime:STRING,local_datetime:STRING,t:STRING,weather_desc:STRING,hu:STRING,ws:STRING,tp:STRING>>>>>"
)

# 4. Eksekusi Logika Flatting Data menggunakan Spark SQL
# 💡 KOREKSI: CAST(value AS STRING) ditambahkan karena Kafka mengirim data dalam format biner (binary)
df_bronze = spark.sql(f"""
    WITH base_parsed AS (
        SELECT from_json(CAST(value AS STRING), '{json_ddl_schema}') AS parsed, timestamp AS kafka_timestamp
        FROM raw_kafka_bmkg
    ),
    flattened_lokasi AS (        
        SELECT 
            kafka_timestamp,
            parsed.lokasi.provinsi AS provinsi,
            parsed.lokasi.kotkab AS kotkab,
            parsed.lokasi.kecamatan AS kecamatan,
            parsed.lokasi.desa AS desa,
            parsed.lokasi.lon AS lon,
            parsed.lokasi.lat AS lat,
            parsed.lokasi.timezone AS timezone,
            parsed.data AS array_data
        FROM base_parsed
    )
    SELECT 
        f.kafka_timestamp,
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

# 5. Konfigurasi Target Penyimpanan di MinIO (Format Parquet)
output_path = "s3a://etl-data/data-lake/bronze/bmkg"
checkpoint_path = "s3a://etl-data/data-lake/checkpoints/bronze_bmkg"

print(f"🚀 Memulai streaming engine. Menulis data dari Kafka ke: {output_path}")

# 6. Menjalankan Streaming Query ke MinIO
# Catatan: Mode 'overwrite' tidak didukung di streaming. Kita gunakan format default streaming (append).
query = df_bronze.writeStream \
    .format("parquet") \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start(output_path)

# Biarkan proses tetap berjalan mendengarkan Kafka
query.awaitTermination()