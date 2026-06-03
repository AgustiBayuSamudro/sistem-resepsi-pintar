from pyspark.sql import SparkSession
import sys

# Inisialisasi dengan paket eksplisit
spark = SparkSession.builder \
    .appName("Resepsi_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "/opt/spark/scripts/streaming/checkpoints/bmkg/")

# Konfigurasi Hadoop gaya Olist (Terbukti Berhasil)
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

df_undangan = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "undangan_topic")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

query_undangan = (df_undangan.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("json") 
    .option("path", "s3a://etl-data/data-lake/raw/resepsi/undangan/")
    .option("checkpointLocation", "/opt/spark/scripts/streaming/checkpoints/undangan/")
    .trigger(processingTime='5 seconds')
    .start())

df_tamu = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "tamu_topic")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())

query_tamu = (df_tamu.selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("json") 
    .option("path", "s3a://etl-data/data-lake/raw/resepsi/tamu/")
    .option("checkpointLocation", "/opt/spark/scripts/streaming/checkpoints/tamu/")
    .trigger(processingTime='5 seconds')
    .start())

print("Streaming Aktif... Memantau Kafka topic 'undangan' dan 'tamu' ")
spark.streams.awaitTermination()  