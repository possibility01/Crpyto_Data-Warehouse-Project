import pypyodbc as odbc
import sys


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

def create_bronze_coin_market(cursor,connection):
    try: 
        print(f'>>>>>creating {coin_market_table}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")
        
        # Check if table exists
        cursor.execute(f"""
            SELECT OBJECT_ID('{coin_market_table}', 'U')
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f'>>>>> {coin_market_table} Table exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_market_table} ")
            print(f'>>>>> {coin_market_table} Table dropped successfully................✅')
            connection.commit()
        # Create the table
        create_table_sql = f"""
        CREATE TABLE {coin_market_table} (
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
        connection.commit()
        print(f'>>>>> {coin_market_table} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")
        
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encountered while trying to create {coin_market_table}: {e}')
        sys.exit(1)

def create_candle_historical_data(cursor,connection):
    try: 
        print(f'>>>>>creating {coin_historical_data_table}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")
        
        # Check if table exists
        cursor.execute(f"""
            SELECT OBJECT_ID('{coin_historical_data_table}', 'U')
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f'>>>>> {coin_historical_data_table} Table exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_historical_data_table} ")
            print(f'>>>>> {coin_historical_data_table} Table dropped successfully................✅')
            connection.commit()
        # Create the table
        create_table_sql = f"""
        CREATE TABLE {coin_historical_data_table} (
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
        connection.commit()
        print(f'>>>>> {coin_historical_data_table} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")
        
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encountered while trying to create {coin_historical_data_table}: {e}')
        sys.exit(1)

def coin_info_data(cursor, connection):

    print('creating information tables ...................................🔄🔄🔄')

    def platform_info():
        try:
            print(f'>>>>>creating {coin_platform_info}................🔃🔃🔃🔃')
            print("--------------------------------------------------------------")

            cursor.execute(
                "SELECT OBJECT_ID(?, 'U')",
                (coin_platform_info,)
            )
            table_exists = cursor.fetchone()[0]

            if table_exists:
                print(f'>>>>> {coin_platform_info} exists, dropping it................🚮')
                cursor.execute(f"DROP TABLE {coin_platform_info}")
                connection.commit()
                print(f'>>>>> {coin_platform_info} Table dropped successfully................✅')

            create_table_sql = f"""
            CREATE TABLE {coin_platform_info} (
                coin_id NVARCHAR(200) NOT NULL,
                platform NVARCHAR(50),
                contract_address NVARCHAR(200)
            );
            """

            cursor.execute(create_table_sql)
            connection.commit()

            print(f'>>>>> {coin_platform_info} table created successfully................✅✅✅')
            print("--------------------------------------------------------------")

        except odbc.Error as e:
            connection.rollback()
            print(f'❌ Error creating {coin_platform_info}: {e}')
            sys.exit(1)

    try:
        print(f'>>>>>creating {coin_basic_info}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")

        cursor.execute(
            "SELECT OBJECT_ID(?, 'U')",
            (coin_basic_info,)
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f'>>>>> {coin_basic_info} exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_basic_info}")
            connection.commit()
            print(f'>>>>> {coin_basic_info} Table dropped successfully................✅')


        create_table_sql = f"""
        CREATE TABLE {coin_basic_info} (
            id NVARCHAR(200) NOT NULL,
            symbol NVARCHAR(10),
            name NVARCHAR(200),
            preview_listing BIT NOT NULL DEFAULT 1,
            description NVARCHAR(MAX),
            country_origin NVARCHAR(20),
            genesis_date NVARCHAR(20),
            last_updated NVARCHAR(20)
        );
        """

        cursor.execute(create_table_sql)
        connection.commit()

        print(f'>>>>> {coin_basic_info} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")

        platform_info()

    except odbc.Error as e:
        connection.rollback()
        print(f'❌ Error creating {coin_basic_info}: {e}')
        sys.exit(1)


    try:
        print(f'>>>>>creating {coin_categories_info}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")

        cursor.execute(
            "SELECT OBJECT_ID(?, 'U')",
            (coin_categories_info,)
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f'>>>>> {coin_categories_info} exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_categories_info}")
            connection.commit()
            print(f'>>>>> {coin_categories_info} Table dropped successfully................✅')

        create_table_sql = f"""
        CREATE TABLE {coin_categories_info} (
            coin_id NVARCHAR(200) ,
            category NVARCHAR(50)
           
        );
        """

        cursor.execute(create_table_sql)
        connection.commit()

        print(f'>>>>> {coin_categories_info} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")


    except odbc.Error as e:
        connection.rollback()
        print(f'❌ Error creating {coin_categories_info}: {e}')
        sys.exit(1)

    try:
        print(f'>>>>>creating {coin_developer_info}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")

        cursor.execute(
            "SELECT OBJECT_ID(?, 'U')",
            (coin_developer_info,)
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f'>>>>> {coin_developer_info} exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_developer_info}")
            connection.commit()
            print(f'>>>>> {coin_developer_info} Table dropped successfully................✅')


        create_table_sql = f"""
        CREATE TABLE {coin_developer_info} (
            forks BIGINT ,
            stars BIGINT,
            subscribers BIGINT,
            total_issues BIGINT,
            pull_requests_merged BIGINT,
            pull_request_contributors BIGINT,
            code_additions_deletions_4_weeks NVARCHAR(50),
            commit_count_4_weeks BIGINT,
            last_4_weeks_commit_activity_series NVARCHAR(200),
            coin_id NVARCHAR(200)

           
        );
        """

        cursor.execute(create_table_sql)
        connection.commit()

        print(f'>>>>> {coin_developer_info} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")


    except odbc.Error as e:
        connection.rollback()
        print(f'❌ Error creating {coin_developer_info}: {e}')
        sys.exit(1)

    try:
        print(f'>>>>>creating {coin_links_info}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")

        cursor.execute(
            "SELECT OBJECT_ID(?, 'U')",
            (coin_links_info,)
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f'>>>>> {coin_links_info} exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_links_info}")
            connection.commit()
            print(f'>>>>> {coin_links_info} Table dropped successfully................✅')


        create_table_sql = f"""
        CREATE TABLE {coin_links_info} (
            homepage NVARCHAR(200) ,
            whitepaper NVARCHAR(200),
            blockchain_site NVARCHAR(200),
            official_forum_url NVARCHAR(200),
            chat_url NVARCHAR(200),
            announcement_url NVARCHAR(200),
            snapshot_url NVARCHAR(200),
            twitter_screen_name NVARCHAR(50),
            bitcointalk_thread_identifier FLOAT,
            subreddit_url NVARCHAR(200),
            repos_url NVARCHAR(200),
            coin_id NVARCHAR(200)

           
        );
        """


        cursor.execute(create_table_sql)
        connection.commit()

        print(f'>>>>> {coin_links_info} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")


    except odbc.Error as e:
        connection.rollback()
        print(f'❌ Error creating {coin_links_info}: {e}')
        sys.exit(1)


    try:
        print(f'>>>>>creating {coin_ticker_info}................🔃🔃🔃🔃')
        print("--------------------------------------------------------------")

        cursor.execute(
            "SELECT OBJECT_ID(?, 'U')",
            (coin_ticker_info,)
        )
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f'>>>>> {coin_ticker_info} exists, dropping it................🚮')
            cursor.execute(f"DROP TABLE {coin_ticker_info}")
            connection.commit()
            print(f'>>>>> {coin_ticker_info} Table dropped successfully................✅')


        create_table_sql = f"""
        CREATE TABLE {coin_ticker_info} (
        base NVARCHAR(20),
        target NVARCHAR(20),

        last FLOAT,
        volume FLOAT,

        trust_score NVARCHAR(20),
        bid_ask_spread_percentage FLOAT,

        timestamp DATETIME2,
        last_traded_at DATETIME2,
        last_fetch_at DATETIME2,

        is_anomaly BIT,
        is_stale BIT,

        trade_url NVARCHAR(500),
        token_info_url NVARCHAR(500),

        coin_id NVARCHAR(200),
        target_coin_id NVARCHAR(200),

        coin_mcap_usd FLOAT,

        market_name NVARCHAR(200),
        market_identifier NVARCHAR(200),
        market_has_trading_incentive BIT,

        converted_last_btc FLOAT,
        converted_last_eth FLOAT,
        converted_last_usd FLOAT,

        converted_volume_btc FLOAT,
        converted_volume_eth FLOAT,
        converted_volume_usd FLOAT
);


        """


        cursor.execute(create_table_sql)
        connection.commit()

        print(f'>>>>> {coin_ticker_info} table created successfully................✅✅✅')
        print("--------------------------------------------------------------")


    except odbc.Error as e:
        connection.rollback()
        print(f'❌ Error creating {coin_ticker_info}: {e}')
        sys.exit(1)


def main():
        connection = connect_sql_server (driver_name,server_name,database_name)

        try:
            cursor = connection.cursor()

            create_bronze_coin_market(cursor,connection)
           

            create_candle_historical_data(cursor,connection)
            coin_info_data(cursor,connection)
           


        except odbc.Error as e:
            print(f'SQL SERVER ERROR:{e}')

        finally:
            cursor.close()
            connection.close()
            print('connection closed............')

if __name__ == "__main__":
    main()        


