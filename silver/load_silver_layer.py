import pypyodbc as odbc
import pandas as pd
import sys
import time

driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"
coin_market_table = "bronze.coin_market"
coin_historical_data_table = "bronze.candle_historical_data"
coin_basic_info = "bronze.coin_basic_info"
coin_developer_info = "bronze.coin_developer_info"
coin_categories_info = "bronze.coin_categories_info"

coin_links_info = "bronze.coin_links_social"
coin_platform_info = "bronze.coin_platform_info"
coin_link_repo_info = "bronze.coin_links_repo"
coin_link_all_info = "bronze.coin_link_all_info"


tables = [coin_market_table, coin_basic_info, coin_categories_info,
          coin_developer_info, coin_historical_data_table, coin_link_all_info,
          coin_link_repo_info, coin_platform_info, coin_links_info]

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

def querying_bronze_layer(driver,server,database):
    

    print("Getting all the bronze layer table..............................🔃🔃🔃")

    try: 
        print(f"Querying the {tables}  .....🔃🔃🔃")
        connection = connect_sql_server (driver,server,database)


        table_data = {}
        for table in tables:
            query = f''' SELECT * FROM {table}'''
            df = pd.read_sql(query,connection)
            table_data[table] = df
            print (f'''fetched {len(df)} rows from {table}''')

        return table_data
    

    except Exception as e:
        print(f'Error querying top 50 coin {tables}: {e}......................❌❌❌')
        return None 
    
    


def cleaning_table(table_data):

    try:
        
        print('cleaning tables.....................................🔄🔄🔄🔄')


        
        # coin_market = table_data['bronze.coin_market']

        # coin_market['max_supply'] = pd.to_numeric(
        # coin_market['max_supply'], errors='coerce'
        # )

        # float_cols = coin_market.select_dtypes(include='float').columns
        # coin_market.loc[:, float_cols] = coin_market[float_cols].round(2)


        basic_info = table_data['bronze.coin_basic_info']
        basic_info.loc[:, :] = basic_info.fillna('Not available')

        social_links = table_data["bronze.coin_links_social"]
        social_links.loc[:,:] =social_links.fillna('Not available')
    
    except Exception as e:
        print(f'Error while trying to clean :{e}')

    no_clean_tables = [
        'bronze.coin_categories_info',
        'bronze.candle_historical_data',
        'bronze.coin_developer_info',
        'bronze.coin_link_all_info',
        'bronze.coin_links_repo',
        'bronze.coin_platform_info'
    ]

    print(f"Skipped cleaning for {len(no_clean_tables)} tables")

    return table_data

           
def load_data(table_data,driver,server,database):

    print('Loading the data into the tables.....................................🔄🔄🔄')

    

    table_config = {
    "silver.coin_basic_info": {
        "id_col": "id",
        "insert_cols": [
            "id", "symbol", "name", "preview_listing",
            "description", "country_origin",
            "genesis_date", "last_updated"
        ],
        "source_df": table_data['bronze.coin_basic_info']
    },

    "silver.coin_categories_info": {
        "id_col": "coin_id",
        "insert_cols": ["coin_id", "category"],
        "source_df": table_data["bronze.coin_categories_info"]
    },

    "silver.candle_historical_data": {
        "id_col": "coin_id",
        "insert_cols": ['[timestamp]', '[open]', '[high]', '[low]', '[close]',
                'coin_id', 'coin_name', '[datetime]'
        ],
        "source_df": table_data['bronze.candle_historical_data']
    },

    'silver.coin_market': {
        "id_col": "id",
        "insert_cols": [
            "id", "symbol", "name", "image",
            "current_price", "market_cap",
            "market_cap_rank", "fully_diluted_valuation" ,
            "total_volume","high_24h",  "low_24h","price_change_24h","price_change_percentage_24h","market_cap_change_24h" ,
            "market_cap_change_percentage_24h",
            "circulating_supply", "total_supply", "max_supply",
            "ath", "ath_change_percentage",
            "ath_date", "atl"  ,"atl_change_percentage", "atl_date", "last_updated", "last_data_date",
           
        ],
        "source_df":table_data['bronze.coin_market'] 
    },
								
 
        

    "silver.coin_platform_info": {
        "id_col": "coin_id",
        "insert_cols": ["coin_id", "platform", "contract_address"],
        "source_df": table_data['bronze.coin_platform_info']
    },
    "silver.coin_developer_info": {
        "id_col": "coin_id",
        "insert_cols": [
            "forks", "stars", "subscribers", "total_issues",
            "pull_requests_merged", "pull_request_contributors",
            "commit_count_4_weeks", "coin_id"
        ],
        "source_df": table_data['bronze.coin_developer_info']
    },

    "silver.coin_links_social": {
        "id_col": "coin_id",
        "insert_cols": [
            'coin_id', 'snapshot_url', 'twitter_screen_name',
     'facebook_username', 'subreddit_url'

        ],
        "source_df": table_data["bronze.coin_links_social"]
        
        },

    "silver.coin_links_repo": {
        "id_col": "coin_id",
        "insert_cols": [
            'coin_id', 'repo_type', 'url'

        ],
        "source_df": table_data['bronze.coin_links_repo']
        
        },
    "silver.coin_link_all_info": {
        "id_col": "coin_id",
        "insert_cols": [
            'coin_id', 'url', 'link_type'

        ],
        "source_df": table_data['bronze.coin_link_all_info']
        
        }


}
    try:
         
        connection = connect_sql_server(driver,server,database)
        cursor = connection.cursor()
        cursor.fast_executemany = True
        for table, conf in table_config.items():
            source_df = conf['source_df']
            id_col = conf['id_col']
            insert_cols = conf['insert_cols']
            db_cols = conf.get('db_cols', insert_cols)


            placeholders = ",".join("?" * len(insert_cols))
            params = list(source_df.itertuples(index=False, name=None))

            insert_sql = f"""
                            INSERT INTO {table} ({",".join(db_cols)})
                            VALUES ({placeholders})
                        """
            start_time = time.time()         
            try:
                            cursor.executemany(insert_sql, params)
                            connection.commit()
                            
                            elapsed = time.time() - start_time
                            print(f'{len(params)} rows inserted in {table}.................... in {elapsed:.2f} seconds')
            except Exception as e:
                            print(f'Error inserting into {table}: {e}............................❌❌❌')
                            print(f'First row sample: {params[0] if params else "No data"}')
                            raise
        

            except Exception as e:
                print(f'error coin while trying to load data into {table}.....................❌❌❌')

    finally:

        cursor.close()
        connection.close()
        print(f'connection closed........................................🔴🔴')
        print("\n✓ All operations completed!.....................................✅✅✅")
        

            

        

       

    





def main():


    bronze_data = querying_bronze_layer(
        driver_name,
        server_name,
        database_name
    )

    cleaning_table(bronze_data)
    load_data(bronze_data,driver_name,server_name,database_name)

        

        
        
        




 


        
    
   
if __name__ == "__main__":
    main()