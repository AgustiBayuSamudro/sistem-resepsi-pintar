CREATE SCHEMA IF NOT EXISTS minio.gold
WITH(
	location = 's3a://etl-data/trino-warehouse/gold/'
);

SELECT * FROM minio.silver.undangan;
SELECT * FROM minio.silver.tamu;
SELECT * FROM minio.silver.lokasi;
SELECT * FROM minio.silver.cuaca;
SELECT * FROM minio.gold.dim_time;

SELECT * FROM minio.gold.undangan;
SELECT * FROM minio.gold.tamu;
SELECT * FROM minio.gold.lokasi;
SELECT * FROM minio.gold.cuaca;
SELECT * FROM minio.gold.dim_kalender;

SELECT * FROM minio.gold.fact_grafik_kehadiran_perjam;
SELECT * FROM minio.gold.fact_cuaca_harian;
SELECT * FROM minio.gold.fact_kehadiran;
SELECT * FROM minio.gold.fact_keuangan;


CREATE TABLE minio.gold.fact_grafik_kehadiran_perjam (
    date_key DATE,
    jam INT,
    tamu_datang BIGINT
)
WITH (
    external_location = 's3a://etl-data/data-lake/gold/fact_grafik_kehadiran_perjam/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.fact_cuaca_harian (
    date_key DATE,
    lokasi_id VARCHAR, 
    suhu_minimum INTEGER,
    suhu_maksimum INTEGER,
    rata_rata_suhu DOUBLE,
    rata_rata_kelembapan DOUBLE,
    kondisi_cuaca_dominan VARCHAR
)
WITH (
    external_location = 's3a://etl-data/data-lake/gold/cuaca_harian/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.fact_kehadiran (
    date_key DATE,
    total_kuota_undangan BIGINT,
    total_tamu_hadir BIGINT,
    total_tamu_undangan_belum_hadir BIGINT,
    total_tamu_non_undangan BIGINT
)
WITH (
    external_location = 's3a://etl-data/data-lake/gold/fact_kehadiran/',
    format = 'PARQUET'
);

DROP TABLE IF EXISTS minio.gold.fact_keuangan;

CREATE TABLE minio.gold.fact_keuangan (
    date_key DATE,
    total_amplop_laki BIGINT,
    total_nominal_laki BIGINT,
    total_nominal_perempuan BIGINT,
    total_amplop_perempuan BIGINT,
    total_semua_amplop BIGINT,
    total_semua_nominal BIGINT,
    rata_rata_nominal DOUBLE
)
WITH (
    external_location = 's3a://etl-data/data-lake/gold/keuangan/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.dim_kalender (
    date_key DATE,
    year INT,
    quarter INT,
    month INT,
    day_of_month INT,
    day_of_week INT,
    month_name VARCHAR,
    month_name_short VARCHAR,
    day_name VARCHAR,
    day_name_short VARCHAR,
    weekend_flag VARCHAR
)
WITH(
	external_location = 's3a://etl-data/data-lake/gold/dim_time/',
    format = 'PARQUET'
);


CREATE TABLE minio.gold.undangan(
	kode_undangan   VARCHAR,
    nama            VARCHAR,
    alamat          VARCHAR,
    jenis_kelamin   VARCHAR 
)
WITH(
	external_location = 's3a://etl-data/data-lake/gold/undangan/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.tamu(
	kode_tamu          VARCHAR,
    kode_undangan      VARCHAR,    
    nama               VARCHAR,
    alamat             VARCHAR,
    nominal            INTEGER,
    pihak              VARCHAR 
)
WITH(
	external_location = 's3a://etl-data/data-lake/gold/tamu/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.lokasi(			
    lokasi_id varchar,
	provinsi varchar,
    kabupaten  varchar, 
    kecamatan  varchar,
    desa varchar,
    longitude varchar,
    latitude varchar,
    timezone varchar
)
WITH(
	external_location = 's3a://etl-data/data-lake/gold/lokasi/',
    format = 'PARQUET'
);

CREATE TABLE minio.gold.cuaca(			
    cuaca_id varchar,
	datetime timestamp,
    local_datetime timestamp,
    temperature integer,
    weather_desc varchar,
    humidity integer,
    wind_speed double,
    precipitation double 
)
WITH(
	external_location = 's3a://etl-data/data-lake/gold/cuaca/',
    format = 'PARQUET'
);


WITH parsed_cuaca AS (
    SELECT DISTINCT
        CAST(local_datetime AS DATE) AS tanggal_log,        
        weather_desc,        
        CAST(t AS DOUBLE) AS suhu_val,
        CAST(hu AS DOUBLE) AS kelembapan_val
    FROM minio.silver.cuaca
),

dim_lokasi_prep AS (
    SELECT DISTINCT         
        TO_HEX(MD5(CAST(desa AS VARBINARY))) AS id_lokasi_generated
    FROM minio.silver.lokasi 
    WHERE LOWER(desa) LIKE '%tapanrejo%'
)
SELECT 
    c.tanggal_log AS date_key,
    l.id_lokasi_generated AS id_lokasi,    
    MIN(c.suhu_val) AS suhu_minimum,
    MAX(c.suhu_val) AS suhu_maksimum,
    ROUND(AVG(c.suhu_val), 2) AS rata_rata_suhu,
    ROUND(AVG(c.kelembapan_val), 2) AS rata_rata_kelembapan,    
    MAX(c.weather_desc) AS kondisi_cuaca_dominan
FROM parsed_cuaca c
CROSS JOIN dim_lokasi_prep l
GROUP BY c.tanggal_log, l.id_lokasi_generated
ORDER BY date_key ASC;

SELECT 
	CAST(created_at AS DATE) AS date_key,
	COUNT(CASE WHEN pihak = 'Laki-laki' THEN 1 END) AS total_amplop_laki,
	SUM(CASE WHEN pihak = 'Laki-laki' THEN nominal ELSE 0 END) AS total_nominal_laki,
	COUNT(CASE WHEN pihak = 'Perempuan' THEN 1 END) AS total_amplop_perempuan,
	SUM(CASE WHEN pihak = 'Perempuan' THEN nominal ELSE 0 END) AS total_nominal_perempuan,
	COUNT(kode_tamu) AS total_semua_amplop,
    SUM(nominal) AS total_semua_nominal,
    AVG(nominal) AS rata_rata_nominal
	FROM minio.silver.tamu
WHERE created_at IS NOT NULL
GROUP BY 1
ORDER BY date_key ASC;

WITH master_kalender AS (
    SELECT CAST(tanggal_mentah AS DATE) AS date_raw
    FROM UNNEST(SEQUENCE(DATE '2025-01-01', DATE '2030-12-31', INTERVAL '1' DAY)) AS t(tanggal_mentah)
)


CREATE TABLE minio.gold.dim_time (
    date_key          DATE,
    year              INT,
    quarter           INT,
    month             INT,
    day_of_month      INT,
    day_of_week       INT,
    month_name        VARCHAR,
    month_name_short  VARCHAR,
    day_name          VARCHAR,
    day_name_short    VARCHAR,
    weekend_flag      VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://etl-data/data-lake/gold/dim_time/'
);

SELECT 
	CAST(created_at AS DATE) AS date_key,
	HOUR(created_at) AS jam ,
	COUNT(kode_tamu) AS tamu_datang FROM minio.silver.tamu
WHERE created_at is not null
GROUP BY 1, 2
ORDER BY date_key ASC, jam ASC;


SELECT 
	CAST(COALESCE(tam.created_at, und.created_at) AS DATE) AS date_key,
	COUNT(und.kode_undangan) AS total_kuota_undangan,
	COUNT(tam.kode_tamu) AS total_tamu_hadir,	
    COUNT(CASE WHEN tam.kode_undangan IS NULL THEN 1 END) AS total_tamu_undangan_belum_hadir,      
    COUNT(CASE WHEN und.kode_undangan IS NULL THEN 1 END) AS total_tamu_non_undangan    
FROM minio.silver.undangan AS und
FULL JOIN minio.silver.tamu AS tam ON und.kode_undangan = tam.kode_undangan
GROUP BY 1
ORDER BY date_key ASC;

    SELECT DISTINCT
        CAST(created_at AS DATE) AS key_tanggal,
        EXTRACT(YEAR FROM created_at) AS tahun,	
        EXTRACT(MONTH FROM created_at) AS bulan,
        FORMAT_DATETIME(created_at, 'MMMM') AS nama_bulan,
        EXTRACT(DAY FROM created_at) AS tanggal
    FROM minio.silver.tamu
    WHERE created_at IS NOT NULL
    ORDER BY key_tanggal ASC;
    
SELECT
	kode_undangan,
	nama,
	alamat,	
	jenis_kelamin
FROM minio.silver.undangan;