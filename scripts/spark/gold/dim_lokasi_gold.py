from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DIMENSI LOKASI") \
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
df_raw = spark.read.parquet("s3a://etl-data/data-lake/silver/lokasi/")

# Temp View
df_raw.createOrReplaceTempView("v_lokasi")

# SQL Transform
df_silver = spark.sql("""
    SELECT
        lokasi_id,
        provinsi,
        kabupaten,
        kecamatan ,
        desa,
        longitude,
        latitude,
        timezone
    FROM v_lokasi
""")

output_path = "s3a://etl-data/data-lake/gold/lokasi/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold lokasi tersimpan di: {output_path}")

spark.stop()