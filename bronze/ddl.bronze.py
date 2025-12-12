import pypyodbc as odbc
import sys


driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"
table_name = "bronze.coin_market"
table_name2 = "bronze.candle_historical_data"
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

def create_bronze_coin_market(cursor):
    try: 
        print(f'>>>>>creating {table_name}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")
        
        # Check if table exists
        cursor.execute(f"""
            SELECT OBJECT_ID('{table_name}', 'U')
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f'>>>>> {table_name} Table exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {table_name} ")
            print(f'>>>>> {table_name} Table dropped successfully................✅')
        
        # Create the table
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            id NVARCHAR(200) ,
            symbol NVARCHAR(50),
            name NVARCHAR(200),
            image NVARCHAR(MAX),
            current_price FLOAT,
            market_cap BIGINT,
            market_cap_rank INT,
            fully_diluted_valuation BIGINT,
            total_volume BIGINT,
            high_24h FLOAT,
            low_24h FLOAT,
            price_change_24h FLOAT,
            price_change_percentage_24h FLOAT,
            market_cap_change_24h FLOAT,
            market_cap_change_percentage_24h FLOAT,
            circulating_supply FLOAT,
            total_supply FLOAT,
            max_supply NVARCHAR(200),
            ath FLOAT,
            ath_change_percentage FLOAT,
            ath_date DATETIME2,
            atl FLOAT,
            atl_change_percentage FLOAT,
            atl_date DATETIME2,
            last_updated DATETIME2,
            last_data_date DATETIME2
        );
        """
        cursor.execute(create_table_sql)
        print(f'>>>>> {table_name} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")
        
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encountered while trying to create {table_name}: {e}')
        sys.exit(1)

def create_candle_historical_data(cursor):
    try: 
        print(f'>>>>>creating {table_name2}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")
        
        # Check if table exists
        cursor.execute(f"""
            SELECT OBJECT_ID('{table_name2}', 'U')
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f'>>>>> {table_name2} Table exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {table_name2} ")
            print(f'>>>>> {table_name2} Table dropped successfully................✅')
        
        # Create the table
        create_table_sql = f"""
        CREATE TABLE {table_name2} (
            [timestamp]  BIGINT,
            [open] FLOAT,
            [high] FLOAT,
            [low] FLOAT,
            [close] FLOAT,
            coin_id  NVARCHAR(200),
            coin_name  NVARCHAR(200),
            [datetime]   DATETIME2
           
        );
        """
        cursor.execute(create_table_sql)
        print(f'>>>>> {table_name2} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")
        
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encountered while trying to create {table_name2}: {e}')
        sys.exit(1)

def main():
        connection = connect_sql_server (driver_name,server_name,database_name)

        try:
            cursor = connection.cursor()

            #drop database if exit
            create_bronze_coin_market(cursor)
            connection.commit()

            create_candle_historical_data(cursor)
            connection.commit()


        except odbc.Error as e:
            print(f'SQL SERVER ERROR:{e}')

        finally:
            cursor.close()
            connection.close()
            print('connection closed............')

if __name__ == "__main__":
    main()        


