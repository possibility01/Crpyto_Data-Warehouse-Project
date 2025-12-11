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

def get_new_data(cursor, data, table_name):
    import pandas as pd

    print(f'Getting the latest data date in {table_name}... 🔃🔃🔃')
    try:
        cursor.execute(f'''
            SELECT id, MAX(last_data_date) AS max_date
            FROM {table_name}
            GROUP BY id
        ''')
        existing_latest_data = cursor.fetchall()
    except Exception as e:
        print(f'Failed to get latest data dates from {table_name}: {e} ❌❌')
        existing_latest_data = []

    existing_latest_data_dates = {
        row[0]: pd.to_datetime(row[1]).floor('s') if row[1] else None
        for row in existing_latest_data
    }
    print(f'Got existing latest data dates from {table_name} ✅✅')

    def is_new_data(row):
        coin_id = row['id']
        current_date = row['last_data_date']

        if pd.isna(current_date):
            return False

        existing_date = existing_latest_data_dates.get(coin_id, None)
        if existing_date is None:
            return True

        return current_date > existing_date

    new_data = data[data.apply(is_new_data, axis=1)]
    print(f"\nTotal rows in DataFrame: {len(data)}")
    print(f"New rows to insert: {len(new_data)}")
    return new_data
   

def data_to_delete(cursor):

    print(f'deleting old data from {table_name}................🔃🔃🔃')
    try:

        delete_data = f"""

                    WITH Ranked AS (  SELECT * , 
                            ROW_NUMBER() OVER(PARTITION BY id ORDER BY last_data_date DESC) AS rn
                            FROM {table_name}
                            )
                    DELETE FROM Ranked
                    WHERE rn > 2 """

        cursor.execute(delete_data)

        print("old rows of data deleted....................✅✅✅✅")

    except Exception as e:

        print (f'error encounted when trying to delete old data:{e}....................')


def load_data_into_DB(cursor,data):
        

        try:

            print(f'loading new data into {table_name} table .............................🔃🔃🔃🔃')
            if len(data) > 0:
                load_query = f'''
                        INSERT INTO {table_name} (
                id, symbol, name, image, current_price, market_cap, market_cap_rank,
                fully_diluted_valuation, total_volume, high_24h, low_24h, price_change_24h,
                price_change_percentage_24h, market_cap_change_24h, market_cap_change_percentage_24h,
                circulating_supply, total_supply, max_supply, ath, ath_change_percentage, ath_date,
                atl, atl_change_percentage, atl_date, last_updated, last_data_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        '''
            
                params_list = [
            (
                row['id'], row['symbol'], row['name'], row['image'], row['current_price'],
                row['market_cap'], row['market_cap_rank'], row['fully_diluted_valuation'],
                row['total_volume'], row['high_24h'], row['low_24h'], row['price_change_24h'],
                row['price_change_percentage_24h'], row['market_cap_change_24h'],
                row['market_cap_change_percentage_24h'], row['circulating_supply'],
                row['total_supply'], row['max_supply'], row['ath'], row['ath_change_percentage'],
                row['ath_date'], row['atl'], row['atl_change_percentage'],
                row['atl_date'], row['last_updated'], row['last_data_date']
            )
            for _, row in data.iterrows()
        ]    

                cursor.executemany(load_query,params_list)

                print(f'{len(data)} new rows inserted into {table_name} successfully..............✅✅✅')

            else:
                print ('!!!! No data to insert, all records already exist with the same last data date..................')

        except Exception as e:
            print(f'Error when loading new data into {table_name}:{e}..................❌❌❌')
        







def main():


    try:


        data = connecting_coin_market_api(50)

        data = data_type_change(data)

        getting_last_data_date(data)

        connection = connect_sql_server (driver_name,server_name,database_name)
        cursor = connection.cursor()
       
        get_new_data(cursor,data,table_name)
        connection.commit()

        data_to_delete(cursor)
        connection.commit()

        load_data_into_DB(cursor,data)
        connection.commit()
        




    finally:
        cursor.close()
        connection.close()
        print ('connection closed successfully ................✅✅✅')


        
    
   
if __name__ == "__main__":
    main()