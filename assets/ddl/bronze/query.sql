CREATE SCHEMA IF NOT EXISTS minio.bronze
WITH(
	location = 's3a://etl-data/trino-warehouse/bronze/'
);

SELECT * FROM minio.bronze.bmkg;

DROP TABLE minio.bronze.bmkg;
DROP TABLE minio.bronze.undangan;
DROP TABLE minio.bronze.tamu;

SELECT * FROM minio.bronze.undangan;
SELECT * FROM minio.bronze.tamu;

SELECT COUNT(*) FROM minio.bronze.undangan;
SELECT COUNT(*) FROM minio.bronze.tamu;

SELECT 
	kode_tamu,
	kode_undangan,
	created_at,
	nama,
	alamat,
	nominal,
	pihak
FROM minio.bronze.tamu;

SELECT 
	kode_undangan,
	nama,
	alamat,
	jenis_kelamin FROM minio.bronze.undangan
	
CREATE TABLE minio.bronze.tamu(
	kode_tamu varchar,
	kode_undangan varchar,
	created_at varchar,
	nama varchar,
	alamat varchar,
	nominal varchar,
	pihak varchar
) 
WITH(
	external_location = 's3a://etl-data/data-lake/raw/resepsi/tamu/',
    format = 'JSON'
)

CREATE TABLE minio.bronze.undangan(
	kode_undangan varchar,
	nama varchar,
	alamat varchar,
	jenis_kelamin varchar
) 
WITH(
	external_location = 's3a://etl-data/data-lake/raw/resepsi/undangan/',
    format = 'JSON'
)

CREATE TABLE minio.bronze.bmkg(			
        provinsi VARCHAR,
        kotkab VARCHAR,
        kecamatan VARCHAR,
        desa VARCHAR,
        lon VARCHAR,
        lat VARCHAR,
        timezone VARCHAR,                
        datetime VARCHAR,
        local_datetime VARCHAR,
        t VARCHAR,
        weather_desc VARCHAR,
        hu VARCHAR,
        ws VARCHAR,
        tp VARCHAR
)
WITH(
	external_location = 's3a://etl-data/data-lake/bronze/bmkg/',
    format = 'PARQUET'
);



