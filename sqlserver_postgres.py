import psycopg2
import pytds
from services.connect_db import get_sqlserver_connection, get_postgres_connection, get_table_structure, table_exists_in_pg, database_exists_in_postgres, database_exists_in_sqlserver
from services.config_db import insert_db_config
from services.sync_data import list_tables_in_sqlserver, create_table_in_postgres, copy_data

def main():
    postgres_dbname = None
    sqlserver_dbname = None
    
    while True:
        if not postgres_dbname:
            postgres_dbname = input("Enter the PostgreSQL database name: ")
            postgres_user = input("Enter the PostgreSQL user: ")
            postgres_password = input("Enter the PostgreSQL password: ")
            postgres_host = input("Enter the PostgreSQL host: ")
            postgres_port = input("Enter the PostgreSQL port: ")

        if not sqlserver_dbname:
            sqlserver_dbname = input("Enter the SQL Server database name: ")
            sqlserver_user = input("Enter the SQL Server user: ")
            sqlserver_password = input("Enter the SQL Server password: ")
            sqlserver_host = input("Enter the SQL Server host: ")

        postgres_conn = get_postgres_connection(
            postgres_host, 
            postgres_port, 
            postgres_dbname, 
            postgres_user, 
            postgres_password
        )

        if not database_exists_in_postgres(postgres_conn, postgres_dbname):
            print(f"PostgreSQL database '{postgres_dbname}' does not exist. Please enter a valid database name.")
            postgres_dbname = None
            postgres_conn.close()
            continue

        sqlserver_conn = get_sqlserver_connection(
            sqlserver_host, 
            sqlserver_dbname, 
            sqlserver_user, 
            sqlserver_password
        )

        if not database_exists_in_sqlserver(sqlserver_conn, sqlserver_dbname):
            print(f"SQL Server database '{sqlserver_dbname}' does not exist. Please enter a valid database name.")
            sqlserver_dbname = None
            sqlserver_conn.close()
            continue
        
        # Store database configuration
        insert_db_config('postgres', postgres_host, postgres_port, postgres_user, postgres_password, postgres_dbname)
        insert_db_config('sqlserver', sqlserver_host, '', sqlserver_user, sqlserver_password, sqlserver_dbname)

        # Reconnect to the specific databases
        postgres_conn.close()
        postgres_conn = get_postgres_connection(
            postgres_host, 
            postgres_port, 
            postgres_dbname, 
            postgres_user, 
            postgres_password
        )

        sqlserver_conn.close()
        sqlserver_conn = get_sqlserver_connection(
            sqlserver_host, 
            sqlserver_dbname, 
            sqlserver_user, 
            sqlserver_password
        )

        tables = list_tables_in_sqlserver(sqlserver_conn)
        print("Available tables in SQL Server database:")
        for idx, table in enumerate(tables, start=1):
            print(f"{idx}. {table}")

        while True:
            table_input = input("Enter the table names or select numbers from the list (comma-separated): ")
            table_names = []
            for item in table_input.split(','):
                item = item.strip()
                if item.isdigit():
                    table_index = int(item) - 1
                    if 0 <= table_index < len(tables):
                        table_names.append(tables[table_index])
                    else:
                        print(f"Invalid selection: {item}. Please try again.")
                else:
                    table_names.append(item)

            valid_tables = []
            for table_name in table_names:
                if table_exists_in_pg(postgres_conn, table_name):
                    print(f"Table '{table_name}' already exists in PostgreSQL database. Skipping this table.")
                    continue

                columns = get_table_structure(sqlserver_conn, table_name)
                if columns:
                    valid_tables.append((table_name, columns))
                else:
                    print(f"Table '{table_name}' does not exist in PostgreSQL database. Skipping this table.")

            if valid_tables:
                for table_name, columns in valid_tables:
                    create_table_in_postgres(postgres_conn, table_name, columns)
                    copy_data(sqlserver_conn, postgres_conn, table_name)
                break
            else:
                print("No valid tables selected. Please try again.")

        postgres_conn.close()
        sqlserver_conn.close()

        continue_work = input("Do you want to continue working? (yes/no): ").strip().lower()
        if continue_work != 'yes':
            break
    print('Have a good day, goodbye!')

if __name__ == "__main__":
    main()
