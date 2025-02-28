#------------------------------------------------------------------------------
# Snowpark Python Stored Procedure: update_crypto_data
# Supports multiple cryptocurrencies (BTC, ETH, DOGE) with separate returns
#------------------------------------------------------------------------------

from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.window import Window

def schema_exists(session, schema_name):
    """
    Check if a schema exists in Snowflake.
    """
    query = f"""
        SELECT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.SCHEMATA 
            WHERE SCHEMA_NAME = '{schema_name}'
        ) AS SCHEMA_EXISTS
    """
    return session.sql(query).collect()[0]['SCHEMA_EXISTS']

def create_schema(session):
    """
    Create the ANALYTICS_CRYPTO schema if it does not exist.
    """
    session.sql("CREATE SCHEMA IF NOT EXISTS ANALYTICS_CRYPTO;").collect()

def table_exists(session, schema='', table=''):
    """
    Check if a table exists in Snowflake.
    """
    query = f"""
        SELECT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        ) AS TABLE_EXISTS
    """
    return session.sql(query).collect()[0]['TABLE_EXISTS']

def create_returns_table(session):
    """
    Create the CRYPTO_RETURNS table if it does not exist.
    """
    session.sql("""
        CREATE TABLE IF NOT EXISTS ANALYTICS_CRYPTO.CRYPTO_RETURNS (
            DATE DATE,
            BTC_PRICE FLOAT,
            BTC_PREVIOUS_DAY_PRICE FLOAT,
            BTC_DAILY_RETURN FLOAT,
            BTC_PREVIOUS_WEEK_PRICE FLOAT,
            BTC_WEEKLY_RETURN FLOAT,
            DOGE_PRICE FLOAT,
            DOGE_PREVIOUS_DAY_PRICE FLOAT,
            DOGE_DAILY_RETURN FLOAT,
            DOGE_PREVIOUS_WEEK_PRICE FLOAT,
            DOGE_WEEKLY_RETURN FLOAT,
            ETH_PRICE FLOAT,
            ETH_PREVIOUS_DAY_PRICE FLOAT,
            ETH_DAILY_RETURN FLOAT,
            ETH_PREVIOUS_WEEK_PRICE FLOAT,
            ETH_WEEKLY_RETURN FLOAT,
            UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

def merge_crypto_updates(session):
    """
    Transform RAW.CRYPTO_DATA and calculate daily & weekly returns per crypto.
    Merge new data into ANALYTICS_CRYPTO.CRYPTO_RETURNS.
    """
    session.sql('ALTER WAREHOUSE CRYPTO_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()
 
    
    source = session.table("RAW.CRYPTO_DATA")\
        .with_column_renamed("OBSERVATION_DATE", "DATE")\
        .with_column_renamed("BTC_CLOSE", "BTC_PRICE")\
        .with_column_renamed("DOGE_CLOSE", "DOGE_PRICE")\
        .with_column_renamed("ETH_CLOSE", "ETH_PRICE")
 
   
    window_spec = Window.order_by("DATE")
 
    for crypto in ["BTC", "DOGE", "ETH"]:
        price_col = f"{crypto}_PRICE"
        source = source.with_columns(
            [
                f"{crypto}_PREVIOUS_DAY_PRICE",
                f"{crypto}_DAILY_RETURN",
                f"{crypto}_PREVIOUS_WEEK_PRICE",
                f"{crypto}_WEEKLY_RETURN"
            ],
            [
                F.lag(price_col, 1).over(window_spec),
                ((F.col(price_col) - F.lag(price_col, 1).over(window_spec)) /
                 F.lag(price_col, 1).over(window_spec)) * 100,
                F.lag(price_col, 7).over(window_spec),
                ((F.col(price_col) - F.lag(price_col, 7).over(window_spec)) /
                 F.lag(price_col, 7).over(window_spec)) * 100
            ]
        )
 
    source = source.with_column("UPDATED_AT", F.current_timestamp())
 
    
    required_columns = [
        "DATE", "BTC_PRICE", "BTC_PREVIOUS_DAY_PRICE", "BTC_DAILY_RETURN",
        "BTC_PREVIOUS_WEEK_PRICE", "BTC_WEEKLY_RETURN", "DOGE_PRICE",
        "DOGE_PREVIOUS_DAY_PRICE", "DOGE_DAILY_RETURN", "DOGE_PREVIOUS_WEEK_PRICE",
        "DOGE_WEEKLY_RETURN", "ETH_PRICE", "ETH_PREVIOUS_DAY_PRICE",
        "ETH_DAILY_RETURN", "ETH_PREVIOUS_WEEK_PRICE", "ETH_WEEKLY_RETURN",
        "UPDATED_AT"
    ]
    
    missing_cols = [c for c in required_columns if c not in source.columns]
    if missing_cols:
        raise ValueError(f"Source missing: {missing_cols}")
 
  
    target = session.table("ANALYTICS_CRYPTO.CRYPTO_RETURNS")
    target.merge(
        source,
        (target["DATE"] == source["DATE"]),
        [
            F.when_matched().update({
                "BTC_PRICE": source["BTC_PRICE"],
                "BTC_PREVIOUS_DAY_PRICE": source["BTC_PREVIOUS_DAY_PRICE"],
                "BTC_DAILY_RETURN": source["BTC_DAILY_RETURN"],
                "BTC_PREVIOUS_WEEK_PRICE": source["BTC_PREVIOUS_WEEK_PRICE"],
                "BTC_WEEKLY_RETURN": source["BTC_WEEKLY_RETURN"],
                "DOGE_PRICE": source["DOGE_PRICE"],
                "DOGE_PREVIOUS_DAY_PRICE": source["DOGE_PREVIOUS_DAY_PRICE"],
                "DOGE_DAILY_RETURN": source["DOGE_DAILY_RETURN"],
                "DOGE_PREVIOUS_WEEK_PRICE": source["DOGE_PREVIOUS_WEEK_PRICE"],
                "DOGE_WEEKLY_RETURN": source["DOGE_WEEKLY_RETURN"],
                "ETH_PRICE": source["ETH_PRICE"],
                "ETH_PREVIOUS_DAY_PRICE": source["ETH_PREVIOUS_DAY_PRICE"],
                "ETH_DAILY_RETURN": source["ETH_DAILY_RETURN"],
                "ETH_PREVIOUS_WEEK_PRICE": source["ETH_PREVIOUS_WEEK_PRICE"],
                "ETH_WEEKLY_RETURN": source["ETH_WEEKLY_RETURN"],
                "UPDATED_AT": source["UPDATED_AT"]
            }),
            F.when_not_matched().insert({
                "DATE": source["DATE"],
                "BTC_PRICE": source["BTC_PRICE"],
                "BTC_PREVIOUS_DAY_PRICE": source["BTC_PREVIOUS_DAY_PRICE"],
                "BTC_DAILY_RETURN": source["BTC_DAILY_RETURN"],
                "BTC_PREVIOUS_WEEK_PRICE": source["BTC_PREVIOUS_WEEK_PRICE"],
                "BTC_WEEKLY_RETURN": source["BTC_WEEKLY_RETURN"],
                "DOGE_PRICE": source["DOGE_PRICE"],
                "DOGE_PREVIOUS_DAY_PRICE": source["DOGE_PREVIOUS_DAY_PRICE"],
                "DOGE_DAILY_RETURN": source["DOGE_DAILY_RETURN"],
                "DOGE_PREVIOUS_WEEK_PRICE": source["DOGE_PREVIOUS_WEEK_PRICE"],
                "DOGE_WEEKLY_RETURN": source["DOGE_WEEKLY_RETURN"],
                "ETH_PRICE": source["ETH_PRICE"],
                "ETH_PREVIOUS_DAY_PRICE": source["ETH_PREVIOUS_DAY_PRICE"],
                "ETH_DAILY_RETURN": source["ETH_DAILY_RETURN"],
                "ETH_PREVIOUS_WEEK_PRICE": source["ETH_PREVIOUS_WEEK_PRICE"],
                "ETH_WEEKLY_RETURN": source["ETH_WEEKLY_RETURN"],
                "UPDATED_AT": source["UPDATED_AT"]
            })
        ]
    )
 
    session.sql('ALTER WAREHOUSE CRYPTO_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session) -> str:
    """
    Main function to run the procedure.
    """
    # Create schema if not exists
    if not schema_exists(session, "ANALYTICS_CRYPTO"):
        create_schema(session)

    # Create table if not exists
    if not table_exists(session, schema='ANALYTICS_CRYPTO', table='CRYPTO_RETURNS'):
        create_returns_table(session)

    # Merge updates
    merge_crypto_updates(session)

    return "Crypto analytics updated successfully!"

# Local testing only
if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session))
