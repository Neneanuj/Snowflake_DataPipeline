--stored procedure updates daily & weekly returns only for new records.
CREATE OR REPLACE PROCEDURE UPDATE_CRYPTO_SP()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Update Daily Returns
    INSERT INTO ANALYTICS_CRYPTO.CRYPTO_DAILY_RETURNS
    SELECT 
        symbol, 
        date, 
        price, 
        LAG(price) OVER (PARTITION BY symbol ORDER BY date) AS previous_day_price,
        ((price - previous_day_price) / previous_day_price) * 100 AS daily_return
    FROM RAW_CRYPTODB.CRYPTO_PRICES
    WHERE date > (SELECT MAX(date) FROM ANALYTICS_CRYPTO.CRYPTO_DAILY_RETURNS);

    -- Update Weekly Returns
    INSERT INTO ANALYTICS_CRYPTO.CRYPTO_WEEKLY_RETURNS
    SELECT 
        symbol, 
        date, 
        price, 
        LAG(price, 7) OVER (PARTITION BY symbol ORDER BY date) AS previous_week_price,
        ((price - previous_week_price) / previous_week_price) * 100 AS weekly_return
    FROM RAW_CRYPTODB.CRYPTO_PRICES
    WHERE date > (SELECT MAX(date) FROM ANALYTICS_CRYPTO.CRYPTO_WEEKLY_RETURNS);
    
    RETURN 'Analytics Tables Updated Successfully';
END;
$$;
