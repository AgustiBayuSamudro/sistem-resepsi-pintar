from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KEUANGAN") \
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
        COUNT(CASE WHEN pihak = 'Laki-laki' THEN 1 END) AS total_amplop_laki,
        SUM(CASE WHEN pihak = 'Laki-laki' THEN nominal ELSE 0 END) AS total_nominal_laki,
        COUNT(CASE WHEN pihak = 'Perempuan' THEN 1 END) AS total_amplop_perempuan,
        SUM(CASE WHEN pihak = 'Perempuan' THEN nominal ELSE 0 END) AS total_nominal_perempuan,
        COUNT(kode_tamu) AS total_semua_amplop,
        SUM(nominal) AS total_semua_nominal,
        AVG(nominal) AS rata_rata_nominal
        FROM v_tamu
    WHERE created_at IS NOT NULL
    GROUP BY 1
    ORDER BY date_key ASC;
""")

output_path = "s3a://etl-data/data-lake/gold/keuangan/"

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold fakta keuangan tersimpan di: {output_path}")

spark.stop()