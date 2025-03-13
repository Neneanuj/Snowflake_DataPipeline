from snowflake.snowpark import Session

def create_view_and_stream(session):
   
    view_sql = """
    CREATE OR REPLACE VIEW RAW.CRYPTO_NORMALIZED_V AS
      SELECT * FROM HARMONIZED.CRYPTO_HARMONIZED
    """
    session.sql(view_sql).collect()
    print("View RAW.CRYPTO_NORMALIZED_V created successfully.")

    stream_sql = """
    CREATE OR REPLACE STREAM RAW.CRYPTO_NORMALIZED_V_STREAM
      ON VIEW RAW.CRYPTO_NORMALIZED_V
      SHOW_INITIAL_ROWS = TRUE
    """
    session.sql(stream_sql).collect()
    print("Stream RAW.CRYPTO_NORMALIZED_V_STREAM created successfully.")

    return "View and stream creation complete."

def main():
    
    session = Session.builder.getOrCreate()
    try:
        result = create_view_and_stream(session)
        print(result)
    finally:
        session.close()

if __name__ == "__main__":
    main()
