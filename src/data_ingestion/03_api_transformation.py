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

# # Show the data for Bitcoin (BTC-USD)
# #print(crypto_data['Close']['BTC-USD'])
# Print the first few rows of the DataFrame to inspect the format
# print("First few rows of the data:")
# print(crypto_data.head())
# # Print the DataFrame info to check the structure and data types
# print("\nDataFrame Info (structure and data types):")
# print(crypto_data.info())
# # Print the column names
# print("\nColumn Names:")
# print(crypto_data.columns)