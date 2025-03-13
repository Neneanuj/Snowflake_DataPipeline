from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, to_timestamp, convert_timezone

def transform_crypto_data(session):
    """
    Transforms raw cryptocurrency data and stores it in the HARMONIZED schema.
    
    Steps:
    1. Reads raw data from the RAW schema.
    2. Standardizes column names for different cryptocurrencies.
    3. Combines all cryptocurrencies into a single DataFrame.
    4. Converts timestamps to UTC.
    5. Removes duplicates and writes transformed data into the HARMONIZED schema.
    
    Parameters:
        session: The active Snowflake session.
    
    Returns:
        The transformed Snowpark DataFrame.
    """
    
    # Switch to the HARMONIZED schema
    session.use_schema("HARMONIZED")
    
    # Read data from the RAW layer, assuming the table name is CRYPTO_DATA
    raw_df = session.table("RAW.CRYPTO_DATA")
    
    # Print column names for debugging (optional)
    print("Columns in RAW.CRYPTO_DATA:", raw_df.columns)
    
    # Define the list of cryptocurrency tickers
    tickers = ["BTC-USD", "DOGE-USD", "ETH-USD"]
    
    transformed_dfs = []
    for ticker in tickers:
        # For "BTC-USD", the prefix should be "BTC"
        prefix = ticker.split('-')[0]
        # Select and rename columns using the actual format where the coin name is prefixed
        ticker_df = raw_df.select(
            col("OBSERVATION_DATE").alias("date"),
            lit(ticker).alias("ticker"),
            col(f"{prefix}_OPEN").alias("open"),
            col(f"{prefix}_HIGH").alias("high"),
            col(f"{prefix}_LOW").alias("low"),
            col(f"{prefix}_CLOSE").alias("close"),
            col(f"{prefix}_VOLUME").alias("volume")
        )
        transformed_dfs.append(ticker_df)
    
    # Union all cryptocurrency data into a single DataFrame
    harmonized_df = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        harmonized_df = harmonized_df.union(df)
    
    # Standardize timestamps to UTC
    harmonized_df = harmonized_df.withColumn(
        "date", 
        convert_timezone(lit("UTC"), to_timestamp(col("date")))
    )
    
    # Remove duplicates and write to the new table
    harmonized_df = harmonized_df.drop_duplicates()
    harmonized_df.write.mode("overwrite").save_as_table("CRYPTO_HARMONIZED")
    
    print("Data successfully transformed and loaded into HARMONIZED.CRYPTO_HARMONIZED")
    return harmonized_df

def main(session_params):
    """
    Main function for data harmonization stored procedure.
    
    Parameters:
        session_params: The session parameters provided by Snowflake
    
    Returns:
        A success message string
    """
    # Import required modules
    from snowflake.snowpark import Session
    from snowflake.snowpark.functions import col, lit, to_timestamp, convert_timezone
    
    # Define the transformation function inline to avoid the session parameter issue
    def transform_crypto_data():
        """
        Transforms raw cryptocurrency data for the HARMONIZED schema.
        """
        # Create direct SQL execution to read data from RAW schema
        raw_df = Session.get_active_session().sql("SELECT * FROM RAW.CRYPTO_DATA")
        
        # Define the list of cryptocurrency tickers
        tickers = ["BTC-USD", "DOGE-USD", "ETH-USD"]
        
        transformed_dfs = []
        for ticker in tickers:
            prefix = ticker.split('-')[0]
            ticker_df = raw_df.select(
                col("OBSERVATION_DATE").alias("date"),
                lit(ticker).alias("ticker"),
                col(f"{prefix}_OPEN").alias("open"),
                col(f"{prefix}_HIGH").alias("high"),
                col(f"{prefix}_LOW").alias("low"),
                col(f"{prefix}_CLOSE").alias("close"),
                col(f"{prefix}_VOLUME").alias("volume")
            )
            transformed_dfs.append(ticker_df)
        
        # Union all cryptocurrency data
        harmonized_df = transformed_dfs[0]
        for df in transformed_dfs[1:]:
            harmonized_df = harmonized_df.union(df)
        
        # Standardize timestamps to UTC
        harmonized_df = harmonized_df.withColumn(
            "date", 
            convert_timezone(lit("UTC"), to_timestamp(col("date")))
        )
        
        # Remove duplicates and write to HARMONIZED schema
        harmonized_df = harmonized_df.drop_duplicates()
        harmonized_df.write.mode("overwrite").save_as_table("HARMONIZED.CRYPTO_HARMONIZED")
        
        return "Data successfully transformed and loaded into HARMONIZED.CRYPTO_HARMONIZED"
    
    # Execute the transformation function
    return transform_crypto_data()
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        sample = main(session)
        sample.show()
