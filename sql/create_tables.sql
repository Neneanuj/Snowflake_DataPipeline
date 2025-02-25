CREATE OR REPLACE TABLE RAW_CRYPTO.PRICES (
    symbol STRING,
    date DATE,
    price FLOAT
);

CREATE OR REPLACE TABLE ANALYTICS_CRYPTO.CRYPTO_RETURNS AS 
SELECT 
    symbol, 
    date, 
    price, 
    LAG(price) OVER (PARTITION BY symbol ORDER BY date) AS previous_day_price,
    LAG(price, 7) OVER (PARTITION BY symbol ORDER BY date) AS previous_week_price,
    ((price - previous_day_price) / previous_day_price) * 100 AS daily_return,
    ((price - previous_week_price) / previous_week_price) * 100 AS weekly_return
FROM RAW_CRYPTO.CRYPTO_PRICES;
