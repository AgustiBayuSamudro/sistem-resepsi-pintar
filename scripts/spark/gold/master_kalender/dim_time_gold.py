from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MASTER_DIMENSI_KALENDER") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# SQL Transform
df_gold = spark.sql("""
   WITH master_kalender AS (
        SELECT explode(sequence(to_date('2025-01-01'), to_date('2030-12-31'), interval 1 day)) AS date_raw
    )
    SELECT    
        date_raw AS date_key,
        year(date_raw) AS year,
        quarter(date_raw) AS quarter,
        month(date_raw) AS month,
        dayofmonth(date_raw) AS day_of_month,
        dayofweek(date_raw) AS day_of_week,      
                
        CASE month(date_raw)
            WHEN 1 THEN 'January'   WHEN 2 THEN 'February' WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'     WHEN 5 THEN 'May'      WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'      WHEN 8 THEN 'August'   WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'  WHEN 11 THEN 'November' WHEN 12 THEN 'December'
        END AS month_name,      
        
        CASE month(date_raw)
            WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
            WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
            WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
            WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
        END AS month_name_short,
        
        -- Day Names (Spark default: 1 = Sunday, 2 = Monday, ..., 7 = Saturday)
        CASE dayofweek(date_raw)
            WHEN 2 THEN 'Monday'    WHEN 3 THEN 'Tuesday'  WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'  WHEN 6 THEN 'Friday'   WHEN 7 THEN 'Saturday'
            WHEN 1 THEN 'Sunday'
        END AS day_name,        
        
        CASE dayofweek(date_raw)
            WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
            WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat'
            WHEN 1 THEN 'Sun'
        END AS day_name_short,     
        
        -- Weekend Flag (Spark default: 1 = Sunday, 7 = Saturday)
        CASE 
            WHEN dayofweek(date_raw) IN (1, 7) THEN 'Weekend' 
            ELSE 'Weekday' 
        END AS weekend_flag
    FROM master_kalender
    ORDER BY date_key ASC;
""")

output_path = "s3a://etl-data/data-lake/gold/dim_time/"

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(output_path)

print(f"Berhasil! Data gold dimensi waktu tersimpan di: {output_path}")

spark.stop()