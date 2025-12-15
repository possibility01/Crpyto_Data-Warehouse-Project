
import pypyodbc as odbc
import sys
import requests
import pandas as pd

driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"
coin_market_table = "bronze.coin_market"
coin_historical_data_table = "bronze.candle_historical_data"
coin_basic_info = "bronze.coin_basic_info"
coin_developer_info = "bronze.coin_developer_info"
coin_categories_info = "bronze.coin_categories_info"
coin_ticker_info ="bronze.coin_tickers_info"
coin_links_info = "bronze.coin_links_info"
coin_platform_info = "bronze.coin_platform_info"





def connect_sql_server (driver,server,database):
    

    try:
        print ('>>>creating connection........................🔄🔄🔄🔄')

        connection = odbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;" )

        print("Connected successfully!.......✅✅✅")

        print("--------------------------------------------------------------")
        return connection
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encounted while trying to connect to SQL SERVER: {e}')
        sys.exit(1)

def getting_top50_coins(connection):

    print('Getting coins information...........................🔄🔄🔄')

    try:
        coins_df = pd.read_sql(
            "SELECT DISTINCT id FROM bronze.coin_market WHERE id IS NOT NULL",
            connection
        )

        print(f"Found {len(coins_df)} coins in the database.")
        return coins_df['id'].tolist()

    except Exception as e:
        print(f'❌ Error getting coins from {coin_market_table}: {e}')
        sys.exit(1)


def getting_info_data(coin_id):

    try:
        print(f'Fetching CoinGecko info for {coin_id}........🔄')

        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.json()

    except Exception as e:
        print(f"❌ Error fetching {coin_id}: {e}")
        return None
    
""" def 
    all_data = []

    for index, row in coins_df.iterrows():
        coin_id = row['id']
        result = connecting_coin_market_api(coin_id)
        if result:
            all_data.append(result)
        time.sleep(15) 

    print(f"Fetched data for {len(all_data)} coins from CoinGecko.")

    coin_df = pd.json_normalize(all_data)
    coin_df
 """


def main():

    connection = connect_sql_server (driver_name,server_name,database_name)
    cursor = connection.cursor()
    coin_id = getting_top50_coins(connection)

    getting_info_data(coin_id)



if __name__ == "__main__":
    main() 

