from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Batch-to-Bronze-Undangan-SQL") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

df_raw = spark.read.json("s3a://etl-data/data-lake/raw/resepsi/undangan/")
df_raw.createOrReplaceTempView("raw_undangan")

df_bronze = spark.sql("""
    SELECT 
        kode_undangan,
        nama,
        alamat,
        jenis_kelamin 
    FROM raw_undangan;
""")

output_path = "s3a://etl-data/data-lake/bronze/resepsi/undangan/"
df_bronze.write.mode("overwrite").parquet(output_path)

print(f"Berhasil! Data bronze undangan tersimpan di: {output_path}")
spark.stop()