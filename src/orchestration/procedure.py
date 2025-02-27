
import time
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    """
    Check if a table exists in Snowflake.
    """
    exists = session.sql(f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS").collect()[0]['TABLE_EXISTS']
    return exists

def create_crypto_returns_table(session):
    """
    Create the CRYPTO_RETURNS table if it does not exist.
    """
    _ = session.sql("""
        CREATE TABLE ANALYTICS_CRYPTO.CRYPTO_RETURNS (
            SYMBOL STRING,
            DATE DATE,
            PRICE FLOAT,
            PREVIOUS_DAY_PRICE FLOAT,
            DAILY_RETURN FLOAT,
            PREVIOUS_WEEK_PRICE FLOAT,
            WEEKLY_RETURN FLOAT,
            META_UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """).collect()

def merge_crypto_updates(session):
    """
    Merge new cryptocurrency price data into ANALYTICS_CRYPTO.CRYPTO_RETURNS table.
    Ensures incremental updates using Snowflake's MERGE function.
    """

    # Scale warehouse to handle merge operation
    _ = session.sql('ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    # Source Table (new cryptocurrency data)
    source = session.table('RAW_CRYPTODB.CRYPTO_PRICES')

    # Target Table (analytics data)
    target = session.table('ANALYTICS_CRYPTO.CRYPTO_RETURNS')

    # Compute Daily Returns
    source = source.with_column("PREVIOUS_DAY_PRICE", F.lag("PRICE").over(partition_by="SYMBOL", order_by="DATE"))
    source = source.with_column("DAILY_RETURN", ((F.col("PRICE") - F.col("PREVIOUS_DAY_PRICE")) / F.col("PREVIOUS_DAY_PRICE")) * 100)

    # Compute Weekly Returns
    source = source.with_column("PREVIOUS_WEEK_PRICE", F.lag("PRICE", 7).over(partition_by="SYMBOL", order_by="DATE"))
    source = source.with_column("WEEKLY_RETURN", ((F.col("PRICE") - F.col("PREVIOUS_WEEK_PRICE")) / F.col("PREVIOUS_WEEK_PRICE")) * 100)

    # Define columns to update
    cols_to_update = {c: source[c] for c in ["SYMBOL", "DATE", "PRICE", "PREVIOUS_DAY_PRICE", "DAILY_RETURN", "PREVIOUS_WEEK_PRICE", "WEEKLY_RETURN"]}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    # Merge into CRYPTO_RETURNS table
    target.merge(source, target['DATE'] == source['DATE'], [
        F.when_matched().update(updates),
        F.when_not_matched().insert(updates)
    ])

    # Scale warehouse back down to save cost
    _ = session.sql('ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    """
    Main function that checks table existence and processes incremental updates.
    """

    # Create CRYPTO_RETURNS table if it does not exist
    if not table_exists(session, schema='ANALYTICS_CRYPTO', name='CRYPTO_RETURNS'):
        create_crypto_returns_table(session)

    # Merge new cryptocurrency data
    merge_crypto_updates(session)

    return f"Successfully processed CRYPTO_RETURNS"

# For local debugging
if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        import sys
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
