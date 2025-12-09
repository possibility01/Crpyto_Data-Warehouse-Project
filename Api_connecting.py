import requests
import pandas as pd

def connecting_coin_market_api(per_page):
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": False
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        print(f'The response code is {response}')
        data = response.json()  
        
        print(f"Fetched top {len(data)} coins by marketcap successfully")
        return data

    except Exception as e:
        print(f"Error: {e}")
        return None


# Fetch top 50 coins
result = connecting_coin_market_api(per_page=50)

# Convert list to DataFrame
df = pd.DataFrame(result)
df['last data date'] = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
print(df)