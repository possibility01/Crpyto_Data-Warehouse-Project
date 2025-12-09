import pypyodbc as odbc
import sys


driver_name  = "SQL Server"
server_name = r"Akeelah\SQLEXPRESS"
database_name = "Crypto_DataWarehouse"

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
        print('>>>>>creating bronze coin market table................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")
        
        # Check if table exists
        cursor.execute("""
            SELECT OBJECT_ID('bronze.coin_market', 'U')
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print('>>>>> Table exists, dropping it................🚮')
            cursor.execute("DROP TABLE bronze.coin_market")
            print('>>>>> Table dropped successfully................✅')
        
        # Create the table
        create_table_sql = """
        CREATE TABLE bronze.coin_market (
            id NVARCHAR(200) PRIMARY KEY,
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
            max_supply FLOAT,
            ath FLOAT,
            ath_change_percentage FLOAT,
            ath_date NVARCHAR(50),
            atl FLOAT,
            atl_change_percentage FLOAT,
            atl_date NVARCHAR(50),
            last_updated NVARCHAR(50),
            last_data_date NVARCHAR(50)
        );
        """
        cursor.execute(create_table_sql)
        print('>>>>> bronze coin market table created successfully................✅✅✅')
        print("--------------------------------------------------------------")
        
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encountered while trying to create bronze coin market table: {e}')
        sys.exit(1)



def main():
        connection = connect_sql_server (driver_name,server_name,database_name)

        try:
            cursor = connection.cursor()

            #drop database if exit
            create_bronze_coin_market(cursor)
            connection.commit()


        except odbc.Error as e:
            print(f'SQL SERVER ERROR:{e}')

        finally:
            cursor.close()
            connection.close()
            print('connection closed............')

if __name__ == "__main__":
    main()        


