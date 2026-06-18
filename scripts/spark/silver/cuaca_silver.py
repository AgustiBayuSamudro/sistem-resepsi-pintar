from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cuaca_Silver") \
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
df_raw.createOrReplaceTempView("silver_cuaca")

# SQL Transform
df_silver = spark.sql("""
    SELECT DISTINCT
        hex(md5((local_datetime || weather_desc))) AS cuaca_id,
        CAST(REPLACE(REPLACE(datetime, 'T', ' '), 'Z', '') AS TIMESTAMP) AS datetime,
        CAST(local_datetime AS TIMESTAMP) AS local_datetime,
        CAST(t AS INTEGER) AS temperature,
        weather_desc,
        CAST(hu AS INTEGER) AS humidity,
        CAST(ws AS DOUBLE) AS wind_speed,
        CAST(tp AS DOUBLE) AS precipitation,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM silver_cuaca
""")

output_path = "s3a://etl-data/data-lake/silver/cuaca/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data silver cuaca tersimpan di: {output_path}")

spark.stop()