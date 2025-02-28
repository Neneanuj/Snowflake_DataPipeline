from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
import math

def create_crypto_view(session):
    # 切换到目标 schema，确保 UDF 注册在该 schema 下
    session.use_schema("analytics")
    
    # 注册 Python UDF，用于计算价格波动率
    # 这里注册为临时 UDF，仅在当前会话内有效
    session.udf.register(
         
         return_type=T.FloatType(),
         input_types=["array<float>"],
         name="CALCULATE_VOLATILITY",
         is_permanent=False
    )
    
    # 从 RAW 层加载加密货币数据（假设 RAW.CRYPTO_DATA 已存在）
    crypto_df = session.table("RAW.CRYPTO_DATA").select(
        F.col("OBSERVATION_DATE"),
        # BTC数据转换：计算收益率和调用 UDF 计算波动率
        F.col("BTC_CLOSE"),
        F.col("BTC_OPEN"),
        ((F.col("BTC_CLOSE") - F.col("BTC_OPEN")) / F.col("BTC_OPEN")).alias("BTC_RETURN"),
        F.call_udf("CALCULATE_VOLATILITY", F.array_construct(F.col("BTC_OPEN"), F.col("BTC_CLOSE"))).alias("BTC_VOLATILITY"),
        
        # DOGE数据转换
        F.col("DOGE_CLOSE"),
        F.col("DOGE_OPEN"),
        ((F.col("DOGE_CLOSE") - F.col("DOGE_OPEN")) / F.col("DOGE_OPEN")).alias("DOGE_RETURN"),
        F.call_udf("CALCULATE_VOLATILITY", F.array_construct(F.col("DOGE_OPEN"), F.col("DOGE_CLOSE"))).alias("DOGE_VOLATILITY"),
        
        # ETH数据转换
        F.col("ETH_CLOSE"),
        F.col("ETH_OPEN"),
        ((F.col("ETH_CLOSE") - F.col("ETH_OPEN")) / F.col("ETH_OPEN")).alias("ETH_RETURN"),
        F.call_udf("CALCULATE_VOLATILITY", F.array_construct(F.col("ETH_OPEN"), F.col("ETH_CLOSE"))).alias("ETH_VOLATILITY")
    )
    
    # 创建或替换视图，将转换后的数据保存为一个规范化的视图
    crypto_df.create_or_replace_view("CRYPTO_NORMALIZED_V")
    print("View CRYPTO_NORMALIZED_V created successfully.")

def create_crypto_view_stream(session):
    session.use_schema("analytics")
    _ = session.sql("""
    CREATE OR REPLACE STREAM CRYPTO_NORMALIZED_V_STREAM
    ON VIEW CRYPTO_NORMALIZED_V
    SHOW_INITIAL_ROWS = TRUE
    """).collect()
    print("Stream CRYPTO_NORMALIZED_V_STREAM created successfully.")

def test_crypto_view(session):
    session.use_schema("analytics")
    df = session.table("CRYPTO_NORMALIZED_V").limit(5)
    df.show()

if __name__ == "__main__":
    # 建立 Snowflake Session（确保连接参数已正确配置）
    with Session.builder.getOrCreate() as session:
        create_crypto_view(session)
        create_crypto_view_stream(session)
        test_crypto_view(session)
