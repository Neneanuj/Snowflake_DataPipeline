-- Step 1: Set Up Account-Level Objects
USE ROLE ACCOUNTADMIN;

-- Create Role & Grant Privileges
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE CRYPTO_ROLE;
GRANT ROLE CRYPTO_ROLE TO ROLE SYSADMIN;
GRANT ROLE CRYPTO_ROLE TO USER IDENTIFIER($MY_USER);
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CRYPTO_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE CRYPTO_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE CRYPTO_ROLE;

-- Create Database & Grant Ownership
CREATE OR REPLACE DATABASE CRYPTO_DB;
GRANT OWNERSHIP ON DATABASE CRYPTO_DB TO ROLE CRYPTO_ROLE;

-- Create Warehouse & Grant Ownership
CREATE OR REPLACE WAREHOUSE CRYPTO_WH 
    WAREHOUSE_SIZE = XSMALL 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_ROLE;

-- Step 2: Create Database-Level Objects
USE ROLE CRYPTO_ROLE;
USE WAREHOUSE CRYPTO_WH;
USE DATABASE CRYPTO_DB;

-- Create Schemas
CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- Create File Format
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create External Stage for S3
CREATE OR REPLACE STAGE RAW.CRYPTO_RAW_STAGE
    URL = 's3://nene.bigdata.s3/crypto_data.csv'
    CREDENTIALS = (
        AWS_KEY_ID = 'AWS_ACCESS_KEY_ID'
        AWS_SECRET_KEY = 'AWS_SECRET_ACCESS_KEY'
    )
    FILE_FORMAT = RAW.CSV_FORMAT;

-- Ensure File Format Skips Header
ALTER FILE FORMAT RAW.CSV_FORMAT SET SKIP_HEADER = 1;

-- Create the Raw Table
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
