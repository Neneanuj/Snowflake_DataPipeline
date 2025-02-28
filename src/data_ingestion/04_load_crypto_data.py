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
