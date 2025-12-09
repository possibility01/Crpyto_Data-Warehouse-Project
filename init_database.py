import pypyodbc as odbc
import sys


driver_name  = "SQL SERVER"  
server_name = r"Akeelah\SQLEXPRESS"
database_name = "master"
new_database = "Crypto_DataWarehouse"
schemas = ['bronze','silver','gold'] 

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

def drop_database_if_exist(cursor,dbname):


    try:
        print (f'dropping {dbname} if existed 🚮🚮🚮')
        drop_database = f"""
                IF EXISTS (SELECT 1 FROM sys.databases WHERE name = '{dbname}')
                BEGIN 
                    ALTER DATABASE {dbname} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE {dbname};
                END
            """
        cursor.execute(drop_database)

        print(f"Existing {dbname} DataWarehouse dropped (if existed) successfully🚮🚮🚮")
        print("--------------------------------------------------------------")
    except odbc.Error as e:
        print(f'❌❌📛❌📛❌Error encounted while trying to drop database: {e}')
        sys.exit(1)

def create_datawarehouse(cursor,dbname):
    print(f'creating {dbname}...........................🔃🔃🔃🔃')

    cursor.execute(f'CREATE DATABASE [{dbname}]')
    print(f' {dbname} created successfully........................✅✅✅')
    print("--------------------------------------------------------------")

def create_schemas(cursor,schemas):

        print(f'creating {schemas} schemas...........................🔃🔃🔃🔃')

        for schema in schemas:
            cursor.execute(f'CREATE SCHEMA {schema}')
            print (f'{schema} schema created......✅✅✅')
        print("--------------------------------------------------------------")

def main():

    connection = connect_sql_server (driver_name,server_name,database_name)

    try:

        cursor = connection.cursor()

        #drop database if exit
        drop_database_if_exist(cursor,new_database)
        connection.commit()

        #create database
        create_datawarehouse(cursor,new_database)
        connection.commit()

        cursor.execute(f'USE [{new_database}]')
        print(f'{new_database} IN USE')

        #create schema
        create_schemas(cursor,schemas)
        connection.commit()

    except odbc.Error as e:
        print(f'SQL SERVER ERROR:{e}')

    finally:
        cursor.close()
        connection.close()
        print('connection closed............')

if __name__ == "__main__":
    main()




