USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- Step #1: Create and assign role for Crypto Project
-- ----------------------------------------------------------------------------
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE CRYPTO_ROLE;
GRANT ROLE CRYPTO_ROLE TO ROLE SYSADMIN;
GRANT ROLE CRYPTO_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE CRYPTO_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE CRYPTO_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE CRYPTO_ROLE;

-- ----------------------------------------------------------------------------
-- Step #2: Create Database and Warehouse for Crypto Project
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DATABASE CRYPTO_DB;
GRANT OWNERSHIP ON DATABASE CRYPTO_DB TO ROLE CRYPTO_ROLE;

CREATE OR REPLACE WAREHOUSE CRYPTO_WH 
    WAREHOUSE_SIZE = XSMALL 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_ROLE;
