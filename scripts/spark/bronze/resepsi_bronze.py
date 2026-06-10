import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ==================== KONFIGURASI BRONZE LAYER ====================
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPICS = 'undangan_topic,tamu_topic'

# Menggunakan environment variable, dengan fallback ke nilai default jika kosong
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio') 
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

# Target lokasi penyimpanan di MinIO Data Lake
BRONZE_BASE_PATH = "s3a://etl-data/data-lake/bronze"
CHECKPOINT_BASE_PATH = "s3a://etl-data/checkpoints/bronze_sheets"
# ==================================================================

# 1. Inisialisasi Spark Session dengan Driver Kafka, AWS S3, dan Delta Lake
spark = SparkSession.builder \
    .appName("Kafka-to-Bronze-Sheets") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                  "org.apache.hadoop:hadoop-aws:3.3.4,"
                                  "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Membaca Aliran Data (Stream) Mendukung Multi-Topic Kafka
print(f"🔄 Mengubungkan ke Kafka Broker [{KAFKA_BROKER}] untuk topik: {TOPICS}...")
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transformasi Minimal Layer Bronze
bronze_stream_df = kafka_stream_df.select(
    col("topic").cast("string").alias("kafka_topic"),
    col("partition").cast("integer").alias("kafka_partition"),
    col("offset").cast("long").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("raw_value"), 
    current_timestamp().alias("ingested_at")
)

# 4. Fungsi Sink Berbasis Batch (Sudah Diperbaiki Menggunakan API DataFrame murni)
def write_to_bronze_sink(batch_df, batch_id):
    # Menggunakan API DataFrame langsung untuk mengambil topik unik dalam micro-batch ini
    distinct_topics = batch_df.select("kafka_topic").distinct().collect()
    
    for row in distinct_topics:
        topic_name = row["kafka_topic"]
        
        # Filter data murni milik salah satu topik secara efisien
        filtered_batch = batch_df.filter(col("kafka_topic") == topic_name)
        
        # Lokasi target di MinIO (Contoh: s3a://etl-data/data-kale/bronze/undangan_topic)
        target_path = f"{BRONZE_BASE_PATH}/{topic_name}"
        
        # Simpan ke MinIO menggunakan format Delta Lake
        filtered_batch.write \
            .format("delta") \
            .mode("append") \
            .save(target_path)

print("🚀 Memulai Streaming Engine ke Bronze Layer MinIO...")

# 5. Menjalankan Engine Query Streaming
query = bronze_stream_df.writeStream \
    .foreachBatch(write_to_bronze_sink) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/multi_topic_vault") \
    .start()

# Menjaga proses tetap aktif mendengarkan Kafka
query.awaitTermination()