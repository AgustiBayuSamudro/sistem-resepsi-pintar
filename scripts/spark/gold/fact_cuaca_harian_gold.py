from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CUACA HARIAN") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

df_lokasi = spark.read.parquet("s3a://etl-data/data-lake/silver/lokasi/")
df_cuaca = spark.read.parquet("s3a://etl-data/data-lake/silver/cuaca/")

df_cuaca.createOrReplaceTempView("v_cuaca")
df_lokasi.createOrReplaceTempView("v_lokasi")

# SQL Transform
df_gold = spark.sql("""
    WITH joined_silver AS (
        SELECT 
            CAST(c.local_datetime AS DATE) AS tanggal_log,
            l.lokasi_id, 
            c.temperature,
            c.humidity,
            c.weather_desc
        FROM v_cuaca c
        CROSS JOIN (
            SELECT DISTINCT lokasi_id 
            FROM v_lokasi
            WHERE desa = 'tapanrejo' 
        ) l
    ),
    modus_cuaca AS (    
        SELECT 
            tanggal_log,
            lokasi_id,
            weather_desc AS kondisi_cuaca_dominan,
            ROW_NUMBER() OVER (
                PARTITION BY tanggal_log, lokasi_id 
                ORDER BY COUNT(*) DESC
            ) as rn
        FROM joined_silver
        GROUP BY tanggal_log, lokasi_id, weather_desc
    )
    SELECT 
        j.tanggal_log AS date_key,
        j.lokasi_id, 
        MIN(j.temperature) AS suhu_minimum,
        MAX(j.temperature) AS suhu_maksimum,
        ROUND(AVG(j.temperature), 2) AS rata_rata_suhu,
        ROUND(AVG(j.humidity), 2) AS rata_rata_kelembapan,
        m.kondisi_cuaca_dominan
    FROM joined_silver j
    JOIN modus_cuaca m 
        ON j.tanggal_log = m.tanggal_log 
        AND j.lokasi_id = m.lokasi_id 
        AND m.rn = 1
    GROUP BY j.tanggal_log, j.lokasi_id, m.kondisi_cuaca_dominan
    ORDER BY date_key ASC;
""")

output_path = "s3a://etl-data/data-lake/gold/cuaca_harian/"

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold fakta cuaca harian tersimpan di: {output_path}")

spark.stop()