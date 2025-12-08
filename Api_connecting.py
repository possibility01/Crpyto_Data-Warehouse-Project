import requests
import json
import pandas as pd

def connecting_coin_market_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",       # currency to get prices in
        "order": "market_cap_desc", # order by market cap descending
        "per_page": 5,              # number of coins per page
        "page": 1,                  # page number
        "sparkline": False          # whether to include sparkline data
    }
    
    try:
        # Make the request with parameters
        response = requests.get(url, params=params, timeout=10)  # timeout in seconds
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        
        # Convert response to JSON
        data_ = response.json()
        return data_
    

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return None  # return None if there was an error

data = connecting_coin_market_api()
data


def process_coin_data(coin_dict):
  coin_market_result = {}

  coin_market_result["id"] = coin_dict["id"]
  coin_market_result["symbol"] = coin_dict["symbol"]
  coin_market_result["name"] = coin_dict["name"]
  coin_market_result["current_price"] = coin_dict["current_price"]
  coin_market_result["market_cap"] = coin_dict["market_cap"]
  coin_market_result["market_cap_rank"] = coin_dict["market_cap_rank"]
  coin_market_result["fully_diluted_valuation"] = coin_dict["fully_diluted_valuation"]
  coin_market_result["total_volume"] = coin_dict["total_volume"]
  coin_market_result["high_24h"] = coin_dict["high_24h"]
  coin_market_result["low_24h"] = coin_dict["low_24h"]
  coin_market_result["price_change_24h"] = coin_dict["price_change_24h"]
  coin_market_result["price_change_percentage_24h"] = coin_dict["price_change_percentage_24h"]
  coin_market_result["market_cap_change_24h"] = coin_dict["market_cap_change_24h"]
  coin_market_result["market_cap_change_percentage_24h"] = coin_dict["market_cap_change_percentage_24h"]
  coin_market_result["circulating_supply"] = coin_dict["circulating_supply"]
  coin_market_result["total_supply"] = coin_dict["total_supply"]
  coin_market_result["max_supply"] = coin_dict["max_supply"]
  coin_market_result["ath"] = coin_dict["ath"]
  coin_market_result["ath_change_percentage"] = coin_dict["ath_change_percentage"]
  coin_market_result["ath_date"] = coin_dict["ath_date"]
  coin_market_result["atl"] = coin_dict["atl"]
  coin_market_result["atl_change_percentage"] = coin_dict["atl_change_percentage"]
  coin_market_result["atl_date"] = coin_dict["atl_date"]
  coin_market_result["roi"] = coin_dict["roi"]
  coin_market_result["last_updated"] = coin_dict["last_updated"]

  return coin_market_result

# Process and print the result
processed_coin_data = []

for coin in data:
    processed_coin = process_coin_data(coin)
    processed_coin_data.append(processed_coin)
processed_coin_data

import pandas as pd

df = pd.DataFrame(processed_coin_data)
print(df)