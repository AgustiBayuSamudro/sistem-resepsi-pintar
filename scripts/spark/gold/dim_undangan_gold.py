from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DIM_UNDANGAN_GOLD") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_silver = spark.read.parquet("s3a://etl-data/data-lake/silver/resepsi/undangan/")

# Temp View
df_silver.createOrReplaceTempView("gold_undangan")

# SQL Transform
df_silver = spark.sql("""
    SELECT
        kode_undangan,
        nama,
        alamat,
        jenis_kelamin
    FROM gold_undangan;
""")

output_path = "s3a://etl-data/data-lake/gold/undangan/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold undangan tersimpan di: {output_path}")

spark.stop()