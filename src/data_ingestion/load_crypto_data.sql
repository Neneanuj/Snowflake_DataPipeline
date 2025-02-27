USE ROLE CRYPTO_ROLE;
USE WAREHOUSE CRYPTO_WH;
USE DATABASE CRYPTO_DB;
USE SCHEMA RAW;

-- Ensure file format skips the header row
ALTER FILE FORMAT RAW.CSV_FORMAT
SET SKIP_HEADER = 1;

-- Create the raw table with columns matching the file structure
CREATE OR REPLACE TABLE RAW.CRYPTO_DATA (
    Date TIMESTAMP,
    Close_BTC FLOAT,
    Close_DOGE FLOAT,
    Close_ETH FLOAT,
    High_BTC FLOAT,
    High_DOGE FLOAT,
    High_ETH FLOAT,
    Low_BTC FLOAT,
    Low_DOGE FLOAT,
    Low_ETH FLOAT,
    Open_BTC FLOAT,
    Open_DOGE FLOAT,
    Open_ETH FLOAT,
    Volume_BTC FLOAT,
    Volume_DOGE FLOAT,
    Volume_ETH FLOAT
);

-- Load data from S3 into the raw table
COPY INTO RAW.CRYPTO_DATA
FROM @RAW.CRYPTO_RAW_STAGE
FILE_FORMAT = RAW.CSV_FORMAT
ON_ERROR = 'CONTINUE';

