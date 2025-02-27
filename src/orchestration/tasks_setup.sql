
-- Create Task for Data Ingestion (Fetches new crypto data daily)
CREATE OR REPLACE TASK LOAD_CRYPTO_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs every day at midnight
AS 
CALL LOAD_CRYPTO_DATA_SP();

-- Create Task for Analytics Updates (Executes stored procedure to update analytics)
CREATE OR REPLACE TASK UPDATE_CRYPTO_METRICS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC' -- Runs every day at 2 AM UTC
AS 
CALL UPDATE_CRYPTO_SP();

-- Enable & Start Tasks
ALTER TASK LOAD_CRYPTO_TASK RESUME;
ALTER TASK UPDATE_CRYPTO_METRICS_TASK RESUME;
