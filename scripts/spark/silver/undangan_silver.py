from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Undangan_Silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_raw = spark.read.parquet(
    "s3a://etl-data/data-lake/bronze/resepsi/undangan/"
)

# Temp View
df_raw.createOrReplaceTempView("silver_undangan")

# SQL Transform
df_silver = spark.sql("""
    SELECT
        TRIM(kode_undangan) kode_undangan,
        initcap(TRIM(nama)) AS nama,
        initcap(TRIM(alamat)) AS alamat,
        TRIM(jenis_kelamin) AS jenis_kelamin,
        CAST(now() AS TIMESTAMP) AS created_at,
        CAST(now() AS TIMESTAMP) AS updated_at
    FROM silver_undangan;
""")

output_path = "s3a://etl-data/data-lake/silver/resepsi/undangan/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data silver undangan tersimpan di: {output_path}")

spark.stop()