from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

def create_crypto_view(session):
    session.use_schema("HARMONIZED")
    
    weekly_window = Window.order_by("OBSERVATION_DATE").rows_between(-6, 0)  # 5天窗口
    
    crypto_df = session.table("RAW.CRYPTO_DATA").select(
        F.col("OBSERVATION_DATE"),
        # BTC
        F.call_udf(
            "crypto_db.analytics.calculate_daily_volatility", 
            F.array_construct(F.col("BTC_OPEN"), F.col("BTC_CLOSE"))
        ).alias("BTC_DAILY_VOL"),
        F.call_udf(
            "crypto_db.analytics.calculate_weekly_volatility",
            F.array_agg(F.col("BTC_CLOSE")).over(weekly_window)
        ).alias("BTC_WEEKLY_VOL"),
        # DOGE
        F.call_udf(
            "crypto_db.analytics.calculate_daily_volatility", 
            F.array_construct(F.col("DOGE_OPEN"), F.col("DOGE_CLOSE"))
        ).alias("DOGE_DAILY_VOL"),
        F.call_udf(
            "crypto_db.analytics.calculate_weekly_volatility",
            F.array_agg(F.col("DOGE_CLOSE")).over(weekly_window)
        ).alias("DOGE_WEEKLY_VOL"),
        # ETH
        F.call_udf(
            "crypto_db.analytics.calculate_daily_volatility", 
            F.array_construct(F.col("ETH_OPEN"), F.col("ETH_CLOSE"))
        ).alias("ETH_DAILY_VOL"),
        F.call_udf(
            "crypto_db.analytics.calculate_weekly_volatility",
            F.array_agg(F.col("ETH_CLOSE")).over(weekly_window)
        ).alias("ETH_WEEKLY_VOL")
    )
    
    crypto_df.create_or_replace_view("CRYPTO_NORMALIZED_V")
    print("View created with daily/weekly volatility metrics.")
