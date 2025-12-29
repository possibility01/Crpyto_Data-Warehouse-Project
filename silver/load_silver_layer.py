import pypyodbc as odbc
import pandas as pd
import sys

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
    
    finally:
        connection.close()
        print("queried all the tables and connection closed successfully...........✅✅✅")


def cleaning_table(table_data):

    try:
        
        print('cleaning tables.....................................🔄🔄🔄🔄')


        
        coin_market = table_data['bronze.coin_market']

        coin_market['max_supply'] = pd.to_numeric(
        coin_market['max_supply'], errors='coerce'
        )

        float_cols = coin_market.select_dtypes(include='float').columns
        coin_market.loc[:, float_cols] = coin_market[float_cols].round(2)


        basic_info = table_data['bronze.coin_basic_info']
        basic_info.loc[:, :] = basic_info.fillna('Not available')
    
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

           

        

       

    





def main():


    bronze_data = querying_bronze_layer(
        driver_name,
        server_name,
        database_name
    )

    cleaning_table(bronze_data)
  


        

        
        
        




 


        
    
   
if __name__ == "__main__":
    main()