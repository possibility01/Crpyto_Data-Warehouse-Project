import requests
import pandas as pd
import pypyodbc as odbc
import time
import sys

driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"
top_50_table = "bronze.coin_market"

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

def connect_to_api_get_top50_coin(driver,server,database):

    print("Getting coin information data from API..............................🔃🔃🔃")

    try: 
        print(f"Querying the {top_50_table} for top 50 coins .....🔃🔃🔃")

        connection = connect_sql_server(driver,server,database)
        cursor = connection.cursor()

        query = """
        SELECT DISTINCT id, name
        FROM bronze.coin_market
        WHERE id IS NOT NULL
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        coins_df = pd.DataFrame(rows, columns=["id", "name"])

        coin_ids = coins_df["id"].tolist()

        print(f"Found {len(coins_df)} coins in the database............✅✅✅")

        

        


    except Exception as e:
        print(f'❌❌❌error occurred while trying to querying top 50 coin: {e}')



    print("Connecting to the API to get coin info............................🔄🔄🔄")

    all_coin_data = []

    for coin_id in coin_ids:

        try:

            

            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
            params = {
            "localization": False,  # exclude all languages except 'en'
            "sparkline": False,
            "market_data": False    # skip market data
        }
        
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            all_coin_data.append(data)
            print(f"Fetched {coin_id} successfully")
            return data
        except Exception as e:
            print(f"Error fetching {coin_id}: {e}")
            return None
        
    return all_coin_data




def main():

    connect_to_api_get_top50_coin(driver_name,server_name,database_name)




if __name__ == "__main__":
    main()