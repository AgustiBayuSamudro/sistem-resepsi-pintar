from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lokasi_Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read raw JSON
df_raw = spark.read.parquet(
    "s3a://etl-data/data-lake/bronze/bmkg/"
)

# Temp View
df_raw.createOrReplaceTempView("silver_lokasi")

# SQL Transform
df_silver = spark.sql("""
    SELECT DISTINCT	
        hex(md5((provinsi || kotkab))) AS lokasi_id,
        TRIM(LOWER(provinsi)) AS provinsi,
        TRIM(LOWER(kotkab)) AS kabupaten,
        TRIM(LOWER(kecamatan)) AS kecamatan ,
        TRIM(LOWER(desa)) AS desa,
        TRIM(lon) AS longitude,
        TRIM(lat) AS latitude,
        TRIM(timezone) AS timezone,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM silver_lokasi
""")

output_path = "s3a://etl-data/data-lake/silver/lokasi/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data silver lokasi tersimpan di: {output_path}")

spark.stop()