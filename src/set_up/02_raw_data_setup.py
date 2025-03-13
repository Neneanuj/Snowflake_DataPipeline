import os
from jinja2 import Template
from snowflake.snowpark import Session
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Retrieve S3 and AWS-related parameters from environment variables
bucket_name = os.environ.get("S3_BUCKET_NAME")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_region = os.environ.get("AWS_REGION")

# -------------------------------
# Define Jinja2 SQL Templates
# -------------------------------

# Template 1.1: Create CONFIG.S3_CREDENTIALS table
template_create_table = """
CREATE OR REPLACE TABLE CONFIG.S3_CREDENTIALS (
    PARAM_NAME STRING,
    PARAM_VALUE STRING
)
"""

# Template 1.2: Insert configuration values into CONFIG.S3_CREDENTIALS
template_insert = """
INSERT INTO CONFIG.S3_CREDENTIALS (PARAM_NAME, PARAM_VALUE)
VALUES 
    ('S3_BUCKET_NAME', '{{ bucket }}'),
    ('AWS_ACCESS_KEY_ID', '{{ key_id }}'),
    ('AWS_SECRET_ACCESS_KEY', '{{ secret_key }}'),
    ('AWS_REGION', '{{ region }}')
"""


# 2. Create a CSV file format (skip the first two header rows)
template_file_format = """
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 2
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;
"""

# 3. Create an external stage to load cryptocurrency data stored in S3
template_stage = """
CREATE OR REPLACE STAGE RAW.CRYPTO_RAW_STAGE
    URL = 's3://{{ bucket }}/crypto_data.csv'
    CREDENTIALS = (
         AWS_KEY_ID = '{{ key_id }}',
         AWS_SECRET_KEY = '{{ secret_key }}'
    )
    FILE_FORMAT = RAW.CSV_FORMAT;
"""


# 4. Create the target table for storing raw-layer cryptocurrency data
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
# Main Function: Execute Full Raw Data Setup
# -------------------------------
def main():
    # Establish a Snowflake session (connection details are automatically loaded from the toml file)
    session = Session.builder.getOrCreate()
    
    try:
        # 1. Create the configuration table and insert S3/AWS credentials (rendered using Jinja2 templates)
        execute_sql(session, template_create_table)
        
        # Render the INSERT statement using Jinja2
        insert_template = Template(template_insert)
        rendered_insert = insert_template.render(
            bucket=bucket_name,
            key_id=aws_access_key_id,
            secret_key=aws_secret_access_key,
            region=aws_region
        )
        # Execute the INSERT statement
        execute_sql(session, rendered_insert)
        
        # 2. Create the CSV file format
        execute_sql(session, template_file_format)
        
        # 3. Create the external stage
        stage_template = Template(template_stage)
        stage_sql = stage_template.render(bucket=bucket_name, key_id=aws_access_key_id, secret_key=aws_secret_access_key)
        execute_sql(session, stage_sql)
        
        # 4. Create the target table
        execute_sql(session, template_table)
        
        print("Raw data setup completed successfully.")
    finally:
        session.close()

if __name__ == "__main__":
    main()