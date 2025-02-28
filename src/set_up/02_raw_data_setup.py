import os
from jinja2 import Template
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()

bucket_name = os.environ.get("S3_BUCKET_NAME")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

# -------------------------------
# Define Jinja2 SQL Templates
# -------------------------------

# 1. Create a CSV file format (skip the first two header lines)
template_file_format = """
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 2
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;
"""

# 2. Create an external stage to load cryptocurrency data (CSV file stored in S3)
template_stage = """
CREATE OR REPLACE STAGE RAW.CRYPTO_RAW_STAGE
    URL = 's3://{{ bucket }}/crypto_data.csv'
    CREDENTIALS = (
         AWS_KEY_ID = '{{ key_id }}',
         AWS_SECRET_KEY = '{{ secret_key }}'
    )
    FILE_FORMAT = RAW.CSV_FORMAT;
"""

# 3. Create the target table for storing raw-layer cryptocurrency data
# Assuming the CSV file columns are ordered as follows:
# Date, BTC_CLOSE, DOGE_CLOSE, ETH_CLOSE, BTC_HIGH, DOGE_HIGH, ETH_HIGH, 
# BTC_LOW, DOGE_LOW, ETH_LOW, BTC_OPEN, DOGE_OPEN, ETH_OPEN, BTC_VOLUME, DOGE_VOLUME, ETH_VOLUME
template_table = """
CREATE OR REPLACE TABLE RAW.CRYPTO_DATA (
    OBSERVATION_DATE TIMESTAMP,
    BTC_CLOSE FLOAT,
    DOGE_CLOSE FLOAT,
    ETH_CLOSE FLOAT,
    BTC_HIGH  FLOAT,
    DOGE_HIGH FLOAT,
    ETH_HIGH  FLOAT,
    BTC_LOW   FLOAT,
    DOGE_LOW  FLOAT,
    ETH_LOW   FLOAT,
    BTC_OPEN  FLOAT,
    DOGE_OPEN FLOAT,
    ETH_OPEN  FLOAT,
    BTC_VOLUME FLOAT,
    DOGE_VOLUME FLOAT,
    ETH_VOLUME FLOAT
);
"""

# -------------------------------
# Helper Function: Execute SQL Command
# -------------------------------
def execute_sql(session, sql_command):
    print("Executing SQL:")
    print(sql_command)
    try:
        session.sql(sql_command).collect()
        print("Executed successfully.\n")
    except Exception as e:
        print("Error executing SQL:", e)

# -------------------------------
# Main Function: Execute Raw Data Setup
# -------------------------------
def main():
    # Establish Snowflake Session using automatically loaded configuration from the toml file.
    session = Session.builder.getOrCreate()
    
    try:
        # Execute SQL to create the file format
        execute_sql(session, template_file_format)
        
        # Use Jinja2 to render the stage creation SQL and execute it
        stage_template = Template(template_stage)
        stage_sql = stage_template.render(bucket=bucket_name, key_id=aws_access_key_id, secret_key=aws_secret_access_key)
        execute_sql(session, stage_sql)
        
        # Execute SQL to create the target table
        execute_sql(session, template_table)
        
        print("Raw data setup completed successfully.")
    finally:
        session.close()

if __name__ == "__main__":
    main()
