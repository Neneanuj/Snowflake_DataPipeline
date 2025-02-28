from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
import pytest

def test_merge_procedure(session):
    # 1. 准备测试数据
    test_data = [
        (1, '2023-01-01', 45000.0, 0.15, 3500.0),
        (2, '2023-01-02', 46000.0, 0.16, 3600.0)
    ]
    
    # 创建临时表
    session.sql("CREATE TEMP TABLE RAW_TEST_DATA AS SELECT * FROM VALUES (1, '2023-01-01', 45000.0, 0.15, 3500.0)").collect()
    
    # 2. 执行存储过程
    session.sql("CALL ANALYTICS_CRYPTO.UPDATE_CRYPTO_DATA()").collect()
    
    # 3. 验证结果
    result = session.table("ANALYTICS_CRYPTO.CRYPTO_RETURNS").filter(F.col("DATE") == '2023-01-02').collect()
    
    # 验证计算逻辑
    assert result[0]["BTC_DAILY_RETURN"] == pytest.approx((46000-45000)/45000*100, abs=0.01)
    assert result[0]["DOGE_WEEKLY_RETURN"] is None  # 不足一周数据应为空