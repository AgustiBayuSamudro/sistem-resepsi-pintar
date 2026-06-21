from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DIMENSI CUACA") \
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
df_silver = spark.read.parquet("s3a://etl-data/data-lake/silver/cuaca/")

# Temp View
df_silver.createOrReplaceTempView("v_cuaca")

# SQL Transform
df_silver = spark.sql("""
    SELECT
        cuaca_id,
        datetime,
        local_datetime,
        temperature,
        weather_desc,
        humidity,
        wind_speed,
        precipitation   
    FROM v_cuaca
""")

output_path = "s3a://etl-data/data-lake/gold/cuaca/"

df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold cuaca tersimpan di: {output_path}")

spark.stop()