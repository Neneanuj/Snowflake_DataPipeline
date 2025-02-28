import yfinance as yf

import boto3
import datetime as dt
from io import StringIO
from snowflake.snowpark import Session

def get_s3_config(session):
    """
    Retrieve S3 and AWS configuration details from the configuration table.
    
    Parameters:
        session: The active Snowflake session.
    
    Returns:
        A dictionary containing AWS credentials and S3 bucket information.
    """
    result = session.sql("SELECT PARAM_NAME, PARAM_VALUE FROM CONFIG.S3_CREDENTIALS").collect()
    config = {row["PARAM_NAME"]: row["PARAM_VALUE"] for row in result}
    return config

def main():
    """
    Stored procedure entry function:
    Creates its own session, retrieves AWS parameters from the configuration table, 
    downloads cryptocurrency data, and uploads it to S3.
    
    Returns:
        A string indicating the upload result, e.g., "Data successfully uploaded to S3 bucket".
    """
    # Create a session internally
    session = Session.builder.getOrCreate()
    
    try:
        # Retrieve AWS/S3 configuration from the configuration table
        s3_config = get_s3_config(session)
        AWS_ACCESS_KEY_ID = s3_config.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = s3_config.get("AWS_SECRET_ACCESS_KEY")
        AWS_REGION = s3_config.get("AWS_REGION")
        AWS_BUCKET_NAME = s3_config.get("S3_BUCKET_NAME")
        
        # Define cryptocurrency tickers and the time range for historical data
        crypto_ticker = ["BTC-USD", "ETH-USD", "DOGE-USD"]
        start_date = "2020-01-01"
        end_date = dt.datetime.now()
        
        # Download historical data
        crypto_data = yf.download(crypto_ticker, start_date, end_date)
        crypto_data = crypto_data.reset_index()
        
        # Save the data as a CSV file in memory
        csv_buffer = StringIO()
        crypto_data.to_csv(csv_buffer, index=False)
        
        # Use boto3 to upload the CSV file to S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key='crypto_data.csv',
            Body=csv_buffer.getvalue()
        )
        
        return "Data successfully uploaded to S3 bucket"
    finally:
        # Close the session to clean up resources
        session.close()

if __name__ == "__main__":
    # This block is only for local debugging
    result = main()
    print(result)