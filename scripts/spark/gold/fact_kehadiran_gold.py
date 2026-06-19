from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KEHADIRAN") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_undangan = spark.read.parquet("s3a://etl-data/data-lake/silver/resepsi/undangan/")
df_tamu = spark.read.parquet("s3a://etl-data/data-lake/silver/resepsi/tamu/")

df_undangan.createOrReplaceTempView("v_undangan")
df_tamu.createOrReplaceTempView("v_tamu")

# SQL Transform
df_gold = spark.sql("""
    SELECT 
        CAST(COALESCE(tam.created_at, und.created_at) AS DATE) AS date_key,
        COUNT(und.kode_undangan) AS total_kuota_undangan,
        COUNT(tam.kode_tamu) AS total_tamu_hadir,	
        COUNT(CASE WHEN tam.kode_undangan IS NULL THEN 1 END) AS total_tamu_undangan_belum_hadir,      
        COUNT(CASE WHEN und.kode_undangan IS NULL THEN 1 END) AS total_tamu_non_undangan    
    FROM v_undangan AS und
    FULL JOIN v_tamu AS tam ON und.kode_undangan = tam.kode_undangan
    GROUP BY 1
    ORDER BY date_key ASC;
""")

output_path = "s3a://etl-data/data-lake/gold/fact_kehadiran/"

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold fakta kehadiran tamu undangan tersimpan di: {output_path}")

spark.stop()