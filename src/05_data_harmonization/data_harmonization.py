import time
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, to_timestamp, convert_timezone

def transform_crypto_data(session):
    # 切换到 HARMONIZED schema
    session.use_schema("HARMONIZED")
    
    # 从 RAW 层读取数据，假设表名为 CRYPTO_DATA
    raw_df = session.table("RAW.CRYPTO_DATA")
    
    # 打印列名以便调试（可选）
    print("Columns in RAW.CRYPTO_DATA:", raw_df.columns)
    
    # 定义所有币种列表
    tickers = ["BTC-USD", "DOGE-USD", "ETH-USD"]
    
    transformed_dfs = []
    for ticker in tickers:
        # 对于 "BTC-USD"，prefix 应为 "BTC"
        prefix = ticker.split('-')[0]
        # 根据实际的列名格式（币名在前面），使用 f"{prefix}_OPEN", f"{prefix}_CLOSE" 等
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
    
    # Union 所有币种数据
    harmonized_df = transformed_dfs[0]
    for df in transformed_dfs[1:]:
        harmonized_df = harmonized_df.union(df)
    
    # 标准化时间戳到 UTC
    harmonized_df = harmonized_df.withColumn(
        "date", 
        convert_timezone(lit("UTC"), to_timestamp(col("date")))
    )
    
    # 去重并写入新表
    harmonized_df = harmonized_df.drop_duplicates()
    harmonized_df.write.mode("overwrite").save_as_table("CRYPTO_HARMONIZED")
    
    print("Data successfully transformed and loaded into HARMONIZED.CRYPTO_HARMONIZED")
    return harmonized_df

def main(session):
    harmonized_df = transform_crypto_data(session)
    return harmonized_df.limit(10)

# For local debugging
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        sample = main(session)
        sample.show()
