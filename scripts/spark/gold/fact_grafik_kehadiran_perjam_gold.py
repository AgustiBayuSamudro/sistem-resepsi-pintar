from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GRAFIK_KEDATANGAN_TAMU") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_tamu = spark.read.parquet("s3a://etl-data/data-lake/silver/resepsi/tamu/")

df_tamu.createOrReplaceTempView("v_tamu")

# SQL Transform
df_gold = spark.sql("""
    SELECT 
        CAST(created_at AS DATE) AS date_key,
        HOUR(created_at) AS jam ,
        COUNT(kode_tamu) AS tamu_datang FROM v_tamu
    WHERE created_at is not null
    GROUP BY 1, 2
    ORDER BY date_key ASC, jam ASC;
""")

output_path = "s3a://etl-data/data-lake/gold/fact_grafik_kehadiran_perjam/"

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold fakta grafik kehadiran tamu undangan tersimpan di: {output_path}")

spark.stop()