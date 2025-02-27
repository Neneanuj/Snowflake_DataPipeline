import time
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, to_timestamp, convert_timezone

def transform_crypto_data(session):
    # Use the raw schema
    session.use_schema("RAW")
    
    # Read data from the raw table
    raw_df = session.table("CRYPTO_DATA")
    
    # Create a list of all tickers
    tickers = ["BTC-USD", "DOGE-USD", "ETH-USD"]
    
    # Initialize an empty list to store transformed dataframes
    transformed_dfs = []
    
    # Process each ticker
    for ticker in tickers:
        # Select and rename columns for this ticker
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
    
    # Union all ticker dataframes
    harmonized_df = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        harmonized_df = harmonized_df.union(df)
    
    # Standardize timestamps to UTC
    harmonized_df = harmonized_df.withColumn(
        "date", 
        convert_timezone("UTC", to_timestamp(col("date")))
    )
    
    harmonized_df = harmonized_df.drop_duplicates()
    session.use_schema("HARMONIZED")
    
    # Create or replace the harmonized table
    harmonized_df.write.mode("overwrite").save_as_table("CRYPTO_HARMONIZED")
    
    
    print("Data successfully transformed and loaded into HARMONIZED.CRYPTO_HARMONIZED")
    
    return harmonized_df


    

# Main execution function
def main(session):
    harmonized_df = transform_crypto_data(session)
    
    
    # Return sample of transformed data
    return harmonized_df.limit(10)

# For local debugging
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        main(session)
