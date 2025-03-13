---
id: Crypto-Pipeline-Codelab

title: "Crypto Data Pipeline Architecture"

summary: "A step-by-step guide to implementing a cryptocurrency data pipeline using AWS S3, Snowflake, Python, GitHub Actions, and Airflow."

authors: ["Anuj Nene","Sicheng Bao", "Yung-rou ko"]

categories: ["Data Engineering", "Snowflake", "AWS", "CI/CD"]

tags: ["Python", "Snowflake", "AWS", "Airflow", "Automation"]

---

# 🚀 Crypto Data Pipeline Codelab

---

## **Page 1: Overview**

### **What is Being Built?**
This project demonstrates the creation of a **cryptocurrency data pipeline** that extracts data from the YFinance API, stores it in AWS S3, processes it in Snowflake using Snowpark Python, and automates workflows with Airflow and Snowflake Tasks. The pipeline also includes CI/CD integration with GitHub Actions and environment management using Jinja templates.

### **Key Features**
- **Data Extraction**: Fetch cryptocurrency data using YFinance API.
- **Data Storage**: Store raw data in AWS S3 and ingest it into Snowflake.
- **Data Transformation**: Use Snowpark Python for harmonization and transformation.
- **Analytics**: Build pre-computed analytics tables for insights.
- **Automation**: Orchestrate workflows with Snowflake Tasks and Airflow.
- **Testing & Validation**: Unit tests for UDFs and stored procedures.
- **Environment Management**: Manage configurations with Jinja templates.

### **Prerequisites**
- Python 3.8+ installed (`python --version`)
- AWS account with S3 access
- Snowflake account with appropriate privileges
- GitHub repository setup
- Airflow environment (local or cloud)
- VSCode with Python extensions installed

### **What You Will Need**
1. Python libraries:
   - `yfinance`, `pandas`, `snowflake-connector-python`, `boto3`, `apache-airflow`
2. AWS CLI configured for S3 access
3. Snowflake CLI or Python connector for database interactions
4. GitHub Actions setup for CI/CD
5. Jinja templates for environment management

---

## **Page 2: Setup**

### **Step-by-Step Setup Instructions**

#### **1️⃣ Configure AWS S3**

      load_dotenv()

      AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
      AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
      AWS_REGION = os.getenv('AWS_REGION')
      AWS_BUCKET_NAME = os.getenv('S3_BUCKET_NAME') 


#### **2️⃣ Configure Snowflake**


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

      USE ROLE CRYPTO_ROLE;
      USE WAREHOUSE CRYPTO_WH;
      USE DATABASE CRYPTO_DB;
      
      -- Schemas
      CREATE OR REPLACE SCHEMA EXTERNAL;
      CREATE OR REPLACE SCHEMA RAW;
      CREATE OR REPLACE SCHEMA RAW_POS;
      CREATE OR REPLACE SCHEMA HARMONIZED;
      CREATE OR REPLACE SCHEMA ANALYTICS;

 


---

## **Page 3: Data Ingestion**

      import yfinance as yf
      import os
      import pandas
      import boto3
      import datetime as dt
      from io import StringIO
      from dotenv import load_dotenv 

      load_dotenv()

      AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
      AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
      AWS_REGION = os.getenv('AWS_REGION')
      AWS_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

      #Crypto dataframe
      crypto_ticker = ["BTC-USD", "ETH-USD", "DOGE-USD"]   #Bitcoin, ethereum, dogecoin
      end_date = dt.datetime.now()
      start_date = "2020-01-01"

      # Download historical data 
      crypto_data = yf.download(crypto_ticker, start_date, end_date)
      crypto_data = crypto_data.reset_index()



      #tempfile
      csv_buffer = StringIO()
      crypto_data.to_csv(csv_buffer, index = False)

      #Upload to S3
      s3_client = boto3.client('s3',
         aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
         region_name=AWS_REGION       
                              )
      s3_client.put_object(
         Bucket = AWS_BUCKET_NAME,
         Key = 'crypto_data.csv',
         Body = csv_buffer.getvalue()
      )

      print(f"Data successfully uploaded to S3 bucket")

### **Summary**
In this step, we will extract cryptocurrency data from the YFinance API and store it as a CSV file in an AWS S3 bucket. This forms the foundation of the pipeline by providing raw data for further processing.

#### **Steps to Implement**
1. Use the YFinance API to fetch historical cryptocurrency data.
2. Save the extracted data as a CSV file locally.
3. Upload the CSV file to an AWS S3 bucket using the `boto3` library.



---

## **Page 4: Raw Data Stage**

### **Summary**
This step involves ingesting raw data from the S3 bucket into Snowflake's RAW_STAGE table. We will also configure Snowflake Streams to track incremental updates in the data.

#### **Steps to Implement**
1. Create an external stage in Snowflake pointing to the S3 bucket.
2. Define the RAW_STAGE table schema in Snowflake.
3. Use Snowflake Streams to track changes in the RAW_STAGE table for incremental updates.

      from snowflake.snowpark import Session

      def load_crypto_data(session):
         """
         Load cryptocurrency data into the RAW.CRYPTO_DATA table using the COPY INTO command from an S3 stage.
         """
         session.use_schema("RAW")
         
         copy_command = """
            COPY INTO RAW.CRYPTO_DATA
            FROM @RAW.CRYPTO_RAW_STAGE
            FILE_FORMAT = RAW.CSV_FORMAT
            ON_ERROR = 'CONTINUE';
         """
         print("Executing COPY command to load crypto data...")
         result = session.sql(copy_command).collect()
         print("COPY command result:")
         for row in result:
            print(row)

      def validate_crypto_data(session):
         """
         Validate the loaded data by printing the first 10 rows of the table.
         """
         print("Validating loaded crypto data:")
         rows = session.table("RAW.CRYPTO_DATA").limit(10).collect()
         for row in rows:
            print(row)

      if __name__ == "__main__":
         # Directly create a session using the automatically loaded configuration
         with Session.builder.getOrCreate() as session:
            load_crypto_data(session)
            validate_crypto_data(session)

---

## **Page 5: Harmonization & Transformation**

### **Summary**
In this step, we will harmonize and transform raw data into a structured format using Snowpark Python. This involves cleaning, normalizing, and structuring the data into a harmonized table.

#### **Steps to Implement**
1. Use Snowpark Python to read from RAW_STAGE.
2. Apply transformations such as currency normalization and missing value handling.
3. Write the transformed data into a CRYPTO_HARMONIZED table.

      for ticker in tickers:
         ticker_df = raw_df.select(
            col("Date").alias("date"),
            lit(ticker).alias("ticker"),
            col(f"Open_{ticker.split('-')[0]}").alias("open"),
            col(f"High_{ticker.split('-')[0]}").alias("high"),
            col(f"Low_{ticker.split('-')[0]}").alias("low"),
            col(f"Close_{ticker.split('-')[0]}").alias("close"),
            col(f"Volume_{ticker.split('-')[0]}").alias("volume")
         )
         transformed_dfs.append(ticker_df)

Session Initialization and Schema Setup: The code connects to Snowflake using Session.builder.getOrCreate() and sets the schema to RAW to access the raw cryptocurrency data.

Reading Raw Data: The data is fetched from the CRYPTO_DATA table in the RAW schema, which contains cryptocurrency information for multiple tickers, including price and volume data.

Processing Multiple Tickers: The code processes data for three specific tickers: "BTC-USD", "DOGE-USD", and "ETH-USD". It iterates through each ticker, selecting relevant columns and renaming them into a standardized format (open, high, low, close, volume).

Unioning Data for All Tickers: The DataFrames for each ticker are combined using the union method to form a single unified DataFrame containing harmonized data for all tickers.

Standardizing Timestamps: The date column is standardized to UTC using convert_timezone("UTC", to_timestamp(col("date"))), ensuring consistency across all timestamps.

Removing Duplicates: The code removes duplicate records from the final DataFrame to ensure that the dataset contains only unique rows.


---

## **Page 6: Creating UDFs**

### **Summary**
We will create User-Defined Functions (UDFs) in this step to perform custom operations on our data:
1. A SQL UDF for currency normalization.
2. A Python UDF for calculating volatility.

#### **Steps to Implement**
1. Define SQL UDFs directly in Snowflake for lightweight transformations.
2. Write Python UDFs using Snowpark for complex calculations like volatility analysis.

      def calculate_daily_volatility(prices: list) -> float:

         if not prices or len(prices) < 2:
            return 0.0
         yesterday = prices[0]
         today = prices[1]
         return abs((today - yesterday) / yesterday)

      def calculate_weekly_volatility(prices: list) -> float:
      
         if not prices or len(prices) < 2:
            return 0.0
         first = prices[0]
         last = prices[-1]
         return abs((last - first) / first)

      calculate_daily_volatility(prices):

      Purpose: This function calculates the daily volatility, which represents how much the price has changed between two consecutive days.
      Logic:
      It checks if there are at least two prices in the list. If not, it returns 0.0 since volatility can't be calculated with insufficient data.
      It then calculates the absolute percentage change between the first two prices in the list (yesterday and today).
      
      
      
      Output: A float value representing the absolute percentage change between the two prices, showing the magnitude of daily price movement.
      calculate_weekly_volatility(prices):

      Purpose: This function calculates the weekly volatility, which measures the price change between the start and the end of a period (often a week).
      Logic:
      It checks if the list has at least two prices. If not, it returns 0.0.
      It calculates the absolute percentage change between the first price (first) and the last price (last) in the list.
      ​
      
      Output: A float value representing the absolute percentage change between the first and last prices in the list, showing the magnitude of weekly price movement.

---

## **Page 7: Analytics & Aggregation**

### **Summary**
This step focuses on building pre-computed analytics tables that aggregate key metrics (e.g., average volatility, daily price changes). These tables will be used for reporting and analysis.

#### **Steps to Implement**
1. Create an analytics table (CRYPTO_ANALYTICS) in Snowflake.
2. Write stored procedures to update analytics tables periodically.


---

## **Page 8: Task Orchestration & Automation**

### **Summary**
Automate tasks such as data ingestion, transformation, and analytics updates using:
1. Snowflake Tasks for scheduling workflows within Snowflake.
2. Airflow DAGs for orchestrating external processes.
3. GitHub Actions for CI/CD integration of Snowpark Python code.

#### **Steps to Implement**
1. Define task dependencies using Snowflake Tasks.
2. Create Airflow DAGs to trigger pipeline steps.
3. Set up GitHub Actions workflows for automated deployments.


---

## **Page 9: Testing & Validation**

### **Summary**
Ensure data quality by writing unit tests for UDFs, stored procedures, and transformations.

#### **Steps to Implement**
1. Write unit tests for SQL UDFs (e.g., currency normalization).
2. Test Python UDFs (e.g., volatility calculations) locally or in a test environment.
3. Validate stored procedures with test datasets.


---

## **Page 10: Environment Management**

### **Summary**
Use Jinja templates to manage configuration files across DEV/PROD environments efficiently.

#### **Steps to Implement**
1. Create Jinja templates for Snowflake objects (tables, stages).
2. Parameterize configurations like database names or schemas.
3. Render templates dynamically during deployment based on environment variables.


