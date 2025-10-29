

SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://shaolistdatastorage.dfs.core.windows.net/olistdata/silver/cleaned/',
        FORMAT = 'PARQUET'
    ) AS result1

--- https://olistdatastorageaccount.blob.core.windows.net/olistdata/silver



create schema gold

create view gold.final 
as 
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://shaolistdatastorage.dfs.core.windows.net/olistdata/silver/cleaned/',
        FORMAT = 'PARQUET'
    ) AS result1



select * from gold.final



CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'buffalo@!23';
CREATE DATABASE SCOPED CREDENTIAL Sheikadmin WITH IDENTITY = 'Managed Identity';

Select * from sys.database_credentials

-- select * from sys.database_credentials

CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://shaolistdatastorage.dfs.core.windows.net/olistdata/gold/',
    CREDENTIAL = Sheikadmin
);


CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'Parquet',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final;





