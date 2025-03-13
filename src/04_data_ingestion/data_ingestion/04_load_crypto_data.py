from snowflake.snowpark import Session

def load_crypto_data(session):
    """
    Load cryptocurrency data into the RAW.CRYPTO_DATA table using the COPY INTO command from an S3 stage.
    This function uses fully qualified names so that no USE command is needed.
    """
    copy_command = """
        COPY INTO RAW.CRYPTO_DATA
        FROM @RAW.CRYPTO_RAW_STAGE
        FILE_FORMAT = RAW.CSV_FORMAT
        ON_ERROR = 'CONTINUE'
    """
    
    result = session.sql(copy_command).collect()
    return f"COPY command executed. Processed {len(result)} rows."

def validate_crypto_data(session):
    """
    Validate the loaded data by retrieving the first 10 rows from RAW.CRYPTO_DATA.
    Uses fully qualified table name.
    """
    rows = session.table("RAW.CRYPTO_DATA").limit(10).collect()
    return rows

def main(session):
    """
    Main function to execute the data loading process.
    """
    message = load_crypto_data(session)
    # Optionally, you can call validate_crypto_data(session) to retrieve sample rows.
    return message

# For local testing
if __name__ == "__main__":
    session = Session.builder.getOrCreate()
    try:
        print(main(session))
    finally:
        session.close()
