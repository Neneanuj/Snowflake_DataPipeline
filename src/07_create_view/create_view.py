from snowflake.snowpark import Session


def create_view(session):
    """
    Executes a series of stored procedures and creates a view and stream in Snowflake.
    
    Steps:
    1. Calls the API transformation stored procedure from the RAW schema.
    2. Calls the data ingestion stored procedure from the RAW schema.
    3. Calls the data harmonization stored procedure from the HARMONIZED schema.
    4. Creates a view (`CRYPTO_NORMALIZED_V`) from the harmonized data.
    5. Creates a stream (`CRYPTO_NORMALIZED_V_STREAM`) on top of the view for change tracking.
    
    Parameters:
        session: The active Snowflake session.
    
    Returns:
        A string message indicating the process is complete.
    """
    
    # Call the API transformation stored procedure from the RAW schema
    session.use_schema("RAW")
    session.sql("CALL API_TRANSFORMATION()").collect()
    print("API_TRANSFORMATION executed from RAW_API schema.")

    # Call the data ingestion stored procedure from the RAW schema
    session.use_schema("RAW")
    session.sql("CALL LOAD_CRYPTO_DATA()").collect()
    print("LOAD_CRYPTO_DATA executed from RAW schema.")

    # Call the data harmonization stored procedure from the HARMONIZED schema
    session.use_schema("HARMONIZED")
    session.sql("CALL TRANSFORM_CRYPTO_DATA()").collect()
    print("TRANSFORM_CRYPTO_DATA executed from HARMONIZED schema.")

    # Finally, create the view and stream
    session.sql("CREATE OR REPLACE VIEW CRYPTO_NORMALIZED_V AS SELECT * FROM CRYPTO_HARMONIZED").collect()
    
    session.sql("""
        CREATE OR REPLACE STREAM CRYPTO_NORMALIZED_V_STREAM
        ON VIEW CRYPTO_NORMALIZED_V
        SHOW_INITIAL_ROWS = TRUE
    """).collect()
    
    print("View and stream created successfully.")

    return "Process complete."

def main(session):
    """
    Main function that triggers the process of executing stored procedures and creating the view and stream.
    
    Parameters:
        session: The active Snowflake session.
    
    Returns:
        A string message indicating the process is complete.
    """
    result = create_view(session)
    return result

# For local debugging
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        print(main(session))
