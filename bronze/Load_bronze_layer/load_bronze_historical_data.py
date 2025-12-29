import requests
import pandas as pd
import time
import sys
import pypyodbc as odbc

driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"


top_50_table = "bronze.coin_market"
table_name = "bronze.candle_historical_data" 


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

def getting_historical_data(driver,server,database):
    """Getting the historical data for the top 50 coins"""

    print("Getting historical data from API..............................🔃🔃🔃")

    try: 
        print(f"Querying the {top_50_table} for top 50 coins .....🔃🔃🔃")
        connection = connect_sql_server (driver,server,database)

        # SQL code to get the top 50 coin from coin_market_table 
        coins_df = pd.read_sql(
            f"""SELECT DISTINCT id, name FROM {top_50_table} WHERE id IS NOT NULL""", 
            connection
        )   

        print(f"Top 50 coin successfully queried .....✅✅✅")

    except Exception as e:
        print(f'Error querying top 50 coin {top_50_table}: {e}......................❌❌❌')
        return None  # Exit early if query fails

    
    # Fetch historical data for all coins
    all_data = []
    
    try:
        for index, row in coins_df.iterrows():
            coin_id = row['id']
            print(f"Fetching data for {coin_id}...")
            
            try:
                response = requests.get(
                    f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc', 
                    params={'vs_currency': 'usd', 'days': '365'}
                )
                response.raise_for_status()
                print(f'Reponse got from the API is {response}')
                
                coin_df = pd.DataFrame(response.json(), columns=['timestamp', 'open', 'high', 'low', 'close'])
                coin_df['coin_id'] = coin_id
                coin_df['coin_name'] = row.get('name', coin_id)
                coin_df['datetime'] = pd.to_datetime(coin_df['timestamp'], unit='ms')
                all_data.append(coin_df)
                
                print(f"✓ Successfully fetched {len(coin_df)} records for {coin_id}...........✅✅✅")
                time.sleep(20)
                print(f"Fetched data for {len(all_data)} coins from CoinGecko.")
                
            except Exception as e:
                print(f"✗ Error fetching {coin_id}: {e}.......................❌❌❌❌")
                continue  

    finally:
        connection.close()

    try:
        print('Combining all historical data for top 50 coins.....................🔃🔃🔃🔃')
        historical_data = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

        
        if not historical_data.empty:
            for coin_id in historical_data['coin_id'].unique():
                coin_data = historical_data[historical_data['coin_id'] == coin_id]
                print(f"\n{'='*80}\nCOIN: {coin_id.upper()} - {coin_data['coin_name'].iloc[0]}\n{'='*80}")
                print(coin_data[['datetime', 'open', 'high', 'low', 'close']].head())
                print(f"Total records: {len(coin_data)}")
            
            return historical_data  
        else:
            print("No data fetched.")
            return None
            
    except Exception as e:
        print(f"✗ Error combining the top 50 historical data: {e}.......................❌❌❌❌")
        return None

def getting_data(driver,server,database,historical_data):

    print(f'Querying the {table_name} table to the previously loaded data.....................🔃🔃🔃')


    try:
        
        print(f'getting the unique coin_id with the number of datapoint and last date data was loaded.....................🔃')

        connection = connect_sql_server (driver,server,database)
        sql = (f"""
        SELECT coin_id, Max([datetime]) AS last_data_load, COUNT(*) AS number_of_data
        FROM {table_name}
        GROUP BY coin_id
        """
        )

        print('connection open....................................🔴🔴🔴')
        cursor = connection.cursor()
        cursor.execute(sql)


        all_coin_rows = cursor.fetchall()

        all_coin = pd.DataFrame(all_coin_rows, columns=['coin_id','last_data_load', 'number_of_data'])

        print(f'{len(all_coin)} coins info gotten from {table_name}................................✅✅✅')


        print('trying to get -----new coins-----,------coins with less than 92 datapoint,-------------coins with full 92 datapoint')
        ohlc_coins = set(historical_data['coin_id'])
        print(f'there are {len(ohlc_coins)} historical data coin.................')

        db_coins = set(all_coin['coin_id'])
        print(f'there are {len(db_coins)} coins in {table_name} ................')

        new_coins = ohlc_coins - db_coins
        print(f'there are {len(new_coins)} coins, are not in {table_name} ................')

        less_92datapoint_coins = all_coin[all_coin['number_of_data'] < 92]['coin_id'].tolist()
        print(f'there are {len(less_92datapoint_coins)} coins,which have less than 92 datapoints in {table_name} ................')

        full_data_coin = all_coin[all_coin['number_of_data'] >= 92]['coin_id'].tolist()
        print(f'there are {len(full_data_coin)} coins,which have 92 datapoints in {table_name} ................')

        historial_new_coin = historical_data[historical_data['coin_id'].isin(new_coins)]

        return historial_new_coin , full_data_coin , less_92datapoint_coins , all_coin

        

    except Exception as e:
        print(f' error while getting coin info from {table_name}:{e}..........................❌❌❌')

    
def load_data(all_coin,historical_data,historial_new_coin, full_data_coin , less_92datapoint_coins,driver,server,database):

    print(f'loading historical data in {table_name}.............................🔃🔄🔃')

    connection = None
    cursor = None
    
    try:

        print(f'{len(historial_new_coin)} new coins loading the new coin data in {table_name}')

        if not historial_new_coin.empty:


            connection = connect_sql_server (driver,server,database)
            cursor = connection.cursor()
            params_list = [
                (
                    row['timestamp'], row['open'], row['high'], row['low'], row['close'],
                    row['coin_id'], row['coin_name'], row['datetime']
                )
                for _, row in historial_new_coin.iterrows()
            ]

            load_query = f'''
            INSERT INTO {table_name} (
                [timestamp], [open], [high], [low], [close],
                coin_id, coin_name, [datetime]
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.executemany(load_query, params_list)
            connection.commit()
            print(f"🆕 {len(params_list)} rows loaded for new coins succesfully..........................✅✅✅")

        else:
            print(f'no new coin data to load into {table_name}.........................🔴🔴')

    except Exception as e:
        print (f'error while loading {len(historial_new_coin)} new coins into {table_name}')



    try:

        print(f'{len(less_92datapoint_coins)}  coins with less 92 datapoints are in {table_name}....................')

        if  less_92datapoint_coins:

            filtered_coins_less92_datapoint_not_historical_data = all_coin[
            (~all_coin['coin_id'].isin(historical_data['coin_id'])) &
            (all_coin['number_of_data'] < 92)
]

            coins_to_delete = filtered_coins_less92_datapoint_not_historical_data['coin_id'].tolist()

            print(f'{len(coins_to_delete)} coins are no longer in the top 50..................')

            if coins_to_delete:
                placeholders = ",".join("?" for _ in coins_to_delete)

                delete_sql = f"""
                    DELETE FROM {table_name}
                    WHERE coin_id IN ({placeholders})
                """

                cursor.execute(delete_sql, coins_to_delete)
                deleted_rows = cursor.rowcount
                connection.commit()

                print(f"🗑️ Deleted {deleted_rows} rows for coins that are not in top 50 anymore")
            else:
                print("ℹ️ No  coins to delete")

          

            filtered_coins_less92_datapoint = all_coin[
            (all_coin['coin_id'].isin(historical_data['coin_id'])) &
            (all_coin['number_of_data'] < 92)
        ]
            
            print(f'{len(filtered_coins_less92_datapoint)}  coins with less 92 datapoints are in  {table_name} and new historical data....................')
            
            historical_data_less_92_incremental = (
            historical_data
            .merge(filtered_coins_less92_datapoint, on='coin_id', how='left')
        )
            
            params_list = [
                (
                    row['timestamp'], row['open'], row['high'], row['low'], row['close'],
                    row['coin_id'], row['coin_name'], row['datetime']
                )
                for _, row in historical_data_less_92_incremental.iterrows()
            ]
            cursor.executemany(load_query, params_list)
            connection.commit()
            print(f"🔄 {len(params_list)} incremental rows inserted for less than 92 datapoint coins")
        else:
            print("ℹ️ No incremental rows to insert for for less than 92 datapoint coins")

    except Exception as e:
        print(f'error while trying to load coins with less than 92 datapoint into {table_name} : {e}')

    historical_full_data = historical_data[historical_data['coin_id'].isin(full_data_coin)]
    if not historical_full_data.empty:

        print(f'trying to load the coins with full 92 datapoints into {table_name}............................🔄🔄🔄')

        try:

            print(f'deleting the oldest data for coins with full 92 datapoints in {table_name}...........................🔄🔄🔄')

            placeholders = ",".join("?" for _ in full_data_coin)
            delete_earliest_sql = f"""
            WITH Ranked AS (
                SELECT coin_id, [datetime],
                       ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY [datetime] ASC) AS rn
                FROM {table_name}
                WHERE coin_id IN ({placeholders})
            )
            DELETE t
            FROM {table_name} t
            INNER JOIN Ranked r ON t.coin_id = r.coin_id AND t.[datetime] = r.[datetime]
            WHERE r.rn = 1
            """
            cursor.execute(delete_earliest_sql)
            connection.commit()
            print(f"✅ {cursor.rowcount} oldest rows deleted for full data coins...................✅✅✅")

        except Exception as e:
            print(f'error while trying to delete the oldest data for coins with full 92 datapoints in {table_name} : {e}')

        try:
            print(f'loading the latest data for coins with full 92 datapoints in {table_name}...........................🔄🔄🔄')
            # Keep only latest row per coin
            historical_full_data_latest = (
                historical_full_data
                .sort_values('datetime')
                .groupby('coin_id', as_index=False)
                .tail(1)
            )
            params_list = [
                (
                    row['timestamp'], row['open'], row['high'], row['low'], row['close'],
                    row['coin_id'], row['coin_name'], row['datetime']
                )
                for _, row in historical_full_data_latest.iterrows()
            ]
            cursor.executemany(load_query, params_list)
            connection.commit()
            print(f"♻️ {len(params_list)} latest rows for coins with full 92 datapoints in {table_name}........................✅✅✅")

        except Exception as e:
            print(f'error while loading latest rows for coins with full 92 datapoints in {table_name}. : {e}')


        finally:
            cursor.close()
            connection.close()
            print("All done ✅ and connection is closed .......................✅✅✅")


def main():


    
    historical_data = getting_historical_data(driver_name,server_name,database_name)  

    if historical_data is not None:
        print(f"\n{'='*80}\nSUMMARY: Successfully fetched data for {len(historical_data['coin_id'].unique())} coins\n{'='*80}")
    else:
        print("\n{'='*80}\nFAILED: No data was fetched\n{'='*80}")


        
    historial_new_coin , full_data_coin ,less_92datapoint_coins , all_coin= getting_data(driver_name,server_name,database_name,historical_data)

    load_data(all_coin,historical_data,historial_new_coin, full_data_coin 
              , less_92datapoint_coins,driver_name,server_name,database_name)
    
    

  
if __name__ == "__main__":
    main() 