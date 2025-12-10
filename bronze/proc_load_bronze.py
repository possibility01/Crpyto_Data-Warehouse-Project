import requests
import pandas as pd
import math
import pypyodbc as odbc
import sys



driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"
table_name = "bronze.coin_market"


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
        
        print(f"Fetched top {len(data)} coins by marketcap successfully......✅✅✅")
        data = pd.DataFrame(data)
        return  data

    except Exception as e:
        print(f"Error: {e}")
        return None
 





def data_type_change(data):

    try:
        print('Adding last data batch column...')
        data['last_data_date'] = pd.to_datetime('today')
        print('Added last_data_date column ✅')


        date_columns  = ['ath_date', 'atl_date', 'last_updated']
        for col in date_columns:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], errors='coerce')
        print('Converted date columns ✅')

        # Convert floats: NaN → None, whole numbers → int string, decimals → string
        def float_string_none(x):
            if isinstance(x, float):
                if math.isnan(x):
                    return None
                elif x.is_integer():
                    return str(int(x))
                else:
                    return format(x, 'f')
            return x

        for col in data.select_dtypes(include=['float']):
            data[col] = data[col].apply(float_string_none)

        print('Converted float columns ✅')
        return data

    except Exception as e:
        print(f'Error in data_type_change: {e} ❌')
        return data

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













def main():


    try:


        data = connecting_coin_market_api(50)

        data = data_type_change(data)

        connection = connect_sql_server (driver_name,server_name,database_name)
        cursor = connection.cursor()

    finally:
        cursor.close()
        connection.close()

        print ('connection closed................')
   
if __name__ == "__main__":
    main()