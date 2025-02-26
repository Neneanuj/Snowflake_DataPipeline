CREATE OR REPLACE TABLE ANALYTICS_CRYPTO.CRYPTO_WEEKLY_RETURNS AS 
SELECT 
    symbol, 
    date, 
    price, 
    LAG(price, 7) OVER (PARTITION BY symbol ORDER BY date) AS previous_week_price,
    ((price - previous_week_price) / previous_week_price) * 100 AS weekly_return
FROM RAW_CRYPTODB.CRYPTO_PRICES;
