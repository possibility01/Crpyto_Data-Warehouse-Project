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

        data = data.where(pd.notnull(data), None)

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
        print(f'Error in data_type_change: {e}............. ❌')
        return data
    

def getting_last_data_date(data):


    print('extracting the last batch data..................🔃🔃🔃')
    data['last_data_date'] = pd.to_datetime(data['last_data_date'], errors='coerce')
    data['last_data_date'] = data['last_data_date'].dt.floor('s')
    print('gotten the last data date......................✅✅✅')
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


def data_to_delete(cursor):

    print(f'deleting old data from {table_name}................🔃🔃🔃')
    try:

        delete_data = f"""

                    WITH Ranked AS (  SELECT * , 
                            ROW_NUMBER() OVER(PARTITION BY id ORDER BY last_data_date) AS rn
                            FROM {table_name}
                            )
                    DELETE FROM Ranked
                    WHERE rn > 1 """

        cursor.execute(delete_data)

        print("old rows of data deleted....................✅✅✅✅")

    except Exception as e:

        print (f'error encounted when trying to delete old data:{e}')







def main():


    try:


        data = connecting_coin_market_api(50)

        data = data_type_change(data)

        data = getting_last_data_date(data)

        connection = connect_sql_server (driver_name,server_name,database_name)
        cursor = connection.cursor()
       
        data_to_delete(cursor)
        connection.commit()




    finally:
        cursor.close()
        connection.close()
        print ('connection closed successfully ................✅✅✅')


        
    
   
if __name__ == "__main__":
    main()