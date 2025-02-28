from snowflake.snowpark import Session

def load_crypto_data(session):
    """
    Load cryptocurrency data into the RAW.CRYPTO_DATA table using the COPY INTO command from an S3 stage.
    """
    session.use_schema("RAW")
    
    copy_command = """
        COPY INTO RAW.CRYPTO_DATA
        FROM @RAW.CRYPTO_S3_INIT
        FILE_FORMAT = RAW.CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """
    
    result = session.sql(copy_command).collect()
    
    return f"COPY command executed. Processed {len(result)} rows."

def validate_crypto_data(session):
    """
    Validate the loaded data by printing the first 10 rows of the table.
    """
    session.use_schema("RAW")
    rows = session.table("RAW.CRYPTO_DATA").limit(10).collect()
    return rows

def main():
    """
    Create a session and execute the data loading process.
    """
    # Create a session within the function
    session = Session.builder.getOrCreate()
    
    try:
        message = load_crypto_data(session)
        return message
    finally:
        # Make sure to close the session
        session.close()

if __name__ == "__main__":
    # For local testing
    print(main())