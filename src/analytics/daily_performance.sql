CREATE OR REPLACE TABLE ANALYTICS_CRYPTO.CRYPTO_DAILY_RETURNS AS 
SELECT 
    symbol, 
    date, 
    price, 
    LAG(price) OVER (PARTITION BY symbol ORDER BY date) AS previous_day_price,
    ((price - previous_day_price) / previous_day_price) * 100 AS daily_return
FROM RAW_CRYPTODB.CRYPTO_PRICES;
