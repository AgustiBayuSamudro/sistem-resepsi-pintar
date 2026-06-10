import json
import cloudscraper  
from pyspark.sql import SparkSession

def run_ingestion():
    # 1. Inisialisasi SparkSession (Tanpa paket Kafka)
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("BMKG_Direct_Ingestion") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    # 2. Konfigurasi Hadoop untuk MinIO
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minio")
    hadoop_conf.set("fs.s3a.secret.key", "minio123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "10")
    hadoop_conf.set("fs.s3a.multipart.size", "104857600")
    hadoop_conf.set("fs.s3a.committer.name", "directory")
    hadoop_conf.set("fs.s3a.committer.staging.tmpdn", "/tmp/spark_staging")
    hadoop_conf.set("fs.s3a.buffer.dir", "/tmp/spark_s3a_buffer")

    # 3. Request Data BMKG via Cloudscraper
    url = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=35.10.05.2006"   
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        print("[*] Mengambil data dari API BMKG memakai Cloudscraper...")
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, headers=headers, timeout=15) 
        response.raise_for_status()        
        raw_weather_data = response.json()
        print("[+] Sukses menembus Cloudflare dan mengambil data JSON BMKG!")

        # 4. Konversi Data JSON Tunggal ke RDD / Spark DataFrame
        # Karena raw_weather_data adalah Python dict, kita bungkus ke dalam list [raw_weather_data]
        rdd_bmkg = sc.parallelize([raw_weather_data])
        df_bmkg = spark.read.json(rdd_bmkg)

        # 5. Tulis Langsung ke MinIO (Format JSON, Mode Append)
        path_bmkg = "s3a://etl-data/data-lake/raw/bmkg/"
        print("[-] Memulai proses penulisan data BMKG langsung ke MinIO...")
        
        df_bmkg.write \
            .mode("append") \
            .json(path_bmkg)
            
        print(f"[+] Ingestion Selesai! Data BMKG sukses disimpan di: {path_bmkg}")

    except Exception as e:
        print(f"[-] Terjadi kesalahan pada proses ETL BMKG: {e}")
        raise e

    finally:
        # Menjamin Spark Session ditutup setelah batch berakhir
        print("[*] Menghentikan Spark Session...")
        spark.stop()

if __name__ == "__main__":
    run_ingestion()