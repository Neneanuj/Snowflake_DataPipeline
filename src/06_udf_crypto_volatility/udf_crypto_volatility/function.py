def calculate_daily_volatility(prices: list) -> float:

    if not prices or len(prices) < 2:
        return 0.0
    yesterday = prices[0]
    today = prices[1]
    return abs((today - yesterday) / yesterday)

def calculate_weekly_volatility(prices: list) -> float:
  
    if not prices or len(prices) < 2:
        return 0.0
    first = prices[0]
    last = prices[-1]
    return abs((last - first) / first)
