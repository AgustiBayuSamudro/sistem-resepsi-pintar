# 🚀 MONITORING TAMU UNDANGAN ACARA RESEPSI

Melakukan pengambilan data api publik dari bmkg dengan library **beautifulsoup4 dan cloudscraper** dan mengambil data tamu undangan via google sheets dengan menerapkan konsep ETL (Extract Transform Load), untuk data source mengambil dari website public (api.bmkg.go.id) dan data google sheets kemudian di ingestion pada data-lake menggunkana **MINIO**. Proyek ini menggunakan konsep **Medallion architecture** dan **Trino Engine** sebagai pengolah data minio yang sudah di transform.

## ✨ Tools yang digunakan
![GitHub Logo](assets/images/tools.png)
* **Language**: Python
* **Workflow Orchestration**: Apache Airflow
* **Distributed Processing**: PySpark 3.5.0
* **Object Storage**: MinIO
* **Database**: PostgreSQL
* **Data Manipulation**: Spark SQL
* **Environment Management**: Python Dotenv
* **Database Driver**: Psycopg2 Binary
* **Containerization**: Docker & Docker Compose

## 📁 struktur folder
![GitHub Logo](assets/images/struktur-folder.png)

## 🚀 Cara Menjalankan (Quick Start)
Pastikan kamu sudah menginstall **Docker** dan **Docker Compose** di laptopmu.

1. **Clone Repositori**
   ```bash
    https://github.com/AgustiBayuSamudro/sistem-resepsi-pintar.git
2. **Build dan jalankan docker**
   ``` bash
   docker compose up --build -d
3. **kemudian buka browser yang di gunakan dan masuk ke url dengan localhost**
   ``` bash
   http://103.196.155.168 atau http://localhost
* **MINIO**
    ``` bash
    http://103.196.155.168:9001/
![GitHub Logo](assets/images/minio.jpeg)
* **AIRFLOW**
    ``` bash
    http://103.196.155.168:8080/
![GitHub Logo](assets/images/airflow.jpeg)
* **SPARK**
    ``` bash
    http://103.196.155.168:8081/
![GitHub Logo](assets/images/spark-master.png)

4. **Buat scema pada trino engine**
Struktur schema pada engine Trino yang mengelola data di MinIO:
![GitHub Logo](assets/images/schema.jpeg)

## 🧪 Cara Pengujian (Testing)
1. **Buat buket pada minio**    
    ![GitHub Logo](assets/images/data-lake.jpeg)
    ``` bash
    dengan username: minio dan password: minio123    
2. **Jalankan airlow sampai status runing**
    ``` bash
    http://103.196.155.168:8080/
    dengan username: admin dan password: admin
3. **Jalankan query sql yang berada pada**
       ![GitHub Logo](assets/images/struktur-sql.png)
4. **Setup Metabase**    
    ![GitHub Logo](assets/images/metabase.png)    