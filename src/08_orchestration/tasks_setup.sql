USE ROLE CRYPTO_ROLE;
USE WAREHOUSE CRYPTO_WH;
USE SCHEMA CRYPTO_DB.raw;
-- Create Task for Data Ingestion (Fetches new crypto data daily)
CREATE OR REPLACE TASK LOAD_CRYPTO_TASK
WAREHOUSE = CRYPTO_WH
SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs every day at midnight UTC
AS 
CALL raw.data_update();



-- Enable & Start Tasks
ALTER TASK LOAD_CRYPTO_TASK RESUME;
-- ALTER TASK UPDATE_CRYPTO_METRICS_TASK RESUME;

-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------

/*---
SHOW TASKS;

-- View Task Execution History
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC;

-- View Upcoming Task Runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;
---*/