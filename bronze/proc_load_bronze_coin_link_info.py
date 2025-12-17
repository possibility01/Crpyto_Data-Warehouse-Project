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
        
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            all_coin_data.append(data)
            print(f"Fetched {coin_id} successfully...............✅✅✅")
            time.sleep(20)
            print(f"Fetched data for {len(all_coin_data)} coins from CoinGecko.")
            
        except Exception as e:
            print(f"Error fetching {coin_id}: {e}")
            return None
        
    return all_coin_data

def normalize_data(data):

    print(f'Normalizing the JSON file gotten from the API.....................🔄🔄🔄🔄')


    try:

        print('getting the developer info..........................🔄🔄🔄')
        

        developer_info = []
        for coin in data:
            dev = coin.get('developer_data', {})
            dev['coin_id'] = coin.get('id')  
            developer_info.append(dev)

        developer_df = pd.DataFrame(developer_info)
        developer_df = developer_df.drop(
            columns=[
                "code_additions_deletions_4_weeks",
                "last_4_weeks_commit_activity_series"
            ],
            errors="ignore"
        )

        print(f'developer info gotten , {len(developer_df)} rows of data gotten..........................✅✅✅')

    except Exception as e:
        print(f' error while trying to get developer info:{e}....................❌❌❌')

    try:

        print('getting the category  info..........................🔄🔄🔄')

        categories_info = []

        for coin in data:
            coin_id = coin.get('id')
            for cat in coin.get('categories', []): 
                categories_info.append({
                    'coin_id': coin_id,
                    'category': cat
                })

        categories_df = pd.DataFrame(categories_info)
        print(f'category info gotten , {len(categories_df)} rows of data gotten..........................✅✅✅')

    except Exception as e:
        print(f' error while trying to get category info:{e}....................❌❌❌')


    try:

        print('getting the platform info..........................🔄🔄🔄')

        platforms_list = []
        for coin in all_data:
            platforms = coin.get('platforms', {})
            if platforms:
                for platform, address in platforms.items():
                    platforms_list.append({
                        'coin_id': coin['id'],
                        'platform': platform,
                        'contract_address': address
                    })

        platforms_df = pd.DataFrame(platforms_list)
        print(f'platform info gotten , {len(platforms_df)} rows of data gotten..........................✅✅✅')

    except Exception as e:
        print(f' error while trying to get platform info:{e}....................❌❌❌')


    try:

        print('getting the general coin info..........................🔄🔄🔄')
        coin_df = pd.json_normalize(data)

        coin_df = coin_df.rename(columns={'description.en': 'description'})

        # Select only relevant parent columns
        parent_columns = [
            'id',
            'symbol',
            'name',
            'preview_listing',
            'description',
            'country_origin',
            'genesis_date',
            'last_updated'
        ]

        coin_df = coin_df.reindex(columns=parent_columns)
        print(f'general coin info gotten , {len(coin_df)} rows of data gotten..........................✅✅✅')

    except Exception as e:
        print(f'error while trying to get general coin info:{e}....................❌❌❌')


    try:

        print('trying the links info..................................🔄🔄🔄🔄')

        links_rows = []
        for coin in data:
            links = coin.get('links', {})
            links['coin_id'] = coin.get('id')  
            links_rows.append(links)

       
        links_df = pd.DataFrame(links_rows)

        print(f'link info info gotten , {len(links_df)} rows of data gotten..........................✅✅✅')


        print("getting the list and dict columns in the link info data............................🔄🔄🔄")

        list_cols = []
        dict_cols = []

        for col in links_df.columns:
            sample = links_df[col].dropna()
            if sample.empty:
                continue

            if sample.apply(lambda x: isinstance(x, list)).any():
                list_cols.append(col)

            if sample.apply(lambda x: isinstance(x, dict)).any():
                dict_cols.append(col)

        print(f"there are {len(list_cols)} list columns:, {list_cols} ............................")
        print(f"there are {len(dict_cols)} dict columns:, {dict_cols} ............................")


        list_dfs = {}

        for col in list_cols:
            tmp = (
                links_df[['coin_id', col]]
                .explode(col)
                .dropna(subset=[col])
                .rename(columns={col: 'url'})
            )
            tmp['link_type'] = col
            list_dfs[col] = tmp
        all_list_links_df = pd.concat(list_dfs.values(), ignore_index=True)
        print(f'{len(all_list_links_df)} rows of data for  ')


        repos_rows = []

        for _, row in links_df[['coin_id', 'repos_url']].dropna().iterrows():
            coin_id = row['coin_id']
            repos = row['repos_url']

            for repo_type, urls in repos.items():
                for url in urls:
                    repos_rows.append({
                        'coin_id': coin_id,
                        'repo_type': repo_type,
                        'url': url
                    })

        repos_df = pd.DataFrame(repos_rows)
        repos_df


        links_scalar_df = links_df[
        ['coin_id', 'snapshot_url', 'twitter_screen_name',
        'facebook_username', 'subreddit_url']]

        links_scalar_df
        
        print

    



def main():

    connect_to_api_get_top50_coin(driver_name,server_name,database_name)




if __name__ == "__main__":
    main()