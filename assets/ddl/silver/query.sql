CREATE SCHEMA IF NOT EXISTS minio.silver
WITH(
	location = 's3a://etl-data/trino-warehouse/silver/'
);

SELECT * FROM minio.bronze.undangan;
SELECT * FROM minio.bronze.tamu;
SELECT * FROM minio.silver.undangan;
SELECT * FROM minio.silver.tamu;
SELECT * FROM minio.silver.lokasi;
SELECT * FROM minio.silver.cuaca;

CREATE TABLE minio.silver.undangan(
	kode_undangan   VARCHAR,
    nama            VARCHAR,
    alamat          VARCHAR,
    jenis_kelamin   VARCHAR,
    created_at timestamp,
    updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/resepsi/undangan/',
    format = 'PARQUET'
);

CREATE TABLE minio.silver.tamu(
	kode_tamu          VARCHAR,
    kode_undangan      VARCHAR,
    created_at         TIMESTAMP,
    nama               VARCHAR,
    alamat             VARCHAR,
    nominal            INTEGER,
    pihak              VARCHAR,
    updated_at 	timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/resepsi/tamu/',
    format = 'PARQUET'
);

CREATE TABLE minio.silver.lokasi(			
    lokasi_id varchar,
	provinsi varchar,
    kabupaten  varchar, 
    kecamatan  varchar,
    desa varchar,
    longitude varchar,
    latitude varchar,
    timezone varchar,
    created_at timestamp,
    updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/lokasi/',
    format = 'PARQUET'
);

CREATE TABLE minio.silver.cuaca(			
    cuaca_id varchar,
	datetime timestamp,
    local_datetime timestamp,
    temperature integer,
    weather_desc varchar,
    humidity integer,
    wind_speed double,
    precipitation double,
    created_at timestamp,
    updated_at timestamp
)
WITH(
	external_location = 's3a://etl-data/data-lake/silver/cuaca/',
    format = 'PARQUET'
);

SELECT
	TRIM(kode_undangan) AS kode_undangan,
	TRIM(kode_tamu) AS kode_tamu ,
	date_parse(created_at, '%d/%m/%Y %H:%i:%s') AS created_at ,
	TRIM(REGEXP_REPLACE(LOWER(nama), '(\b\w)', x -> UPPER(x[1]))) AS name,
	TRIM(REGEXP_REPLACE(LOWER(alamat), '(\b\w)', x -> UPPER(x[1]))) AS alamat,
	CAST(nominal AS INTEGER) AS nominal ,
	TRIM(pihak) AS pihak FROM minio.bronze.tamu;


SELECT
	TRIM(kode_undangan) kode_undangan,
	TRIM(REGEXP_REPLACE(LOWER(nama), '(\b\w)', x -> UPPER(x[1]))) AS nama,
	TRIM(REGEXP_REPLACE(LOWER(alamat), '(\b\w)', x -> UPPER(x[1]))) AS alamat,
	TRIM(jenis_kelamin) AS jenis_kelamin 
FROM minio.bronze.undangan