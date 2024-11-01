import psycopg2
import pytds

def get_sqlserver_connection(server, database, user, password):
    return pytds.connect(
        dsn=server,
        database=database,
        user=user,
        password=password,
    )

def get_postgres_connection(host, port, database, user, password):
    return psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

def get_table_structure(sqlserver_conn, table_name):
    sqlserver_cursor = sqlserver_conn.cursor()

    sqlserver_cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
    """)
    
    columns = sqlserver_cursor.fetchall()
    sqlserver_cursor.close()

    return columns

def table_exists_in_pg(pg_conn, table_name):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'")
    result = pg_cursor.fetchone()
    pg_cursor.close()
    return result is not None

def database_exists_in_postgres(postgres_conn, dbname):
    pg_cursor = postgres_conn.cursor()
    pg_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
    exists = pg_cursor.fetchone() is not None
    pg_cursor.close()
    return exists

def database_exists_in_sqlserver(sqlserver_conn, dbname):
    sqlserver_cursor = sqlserver_conn.cursor()
    sqlserver_cursor.execute(f"SELECT 1 FROM sys.databases WHERE name = '{dbname}'")
    exists = sqlserver_cursor.fetchone() is not None
    sqlserver_cursor.close()
    return exists

def list_tables_in_sqlserver(sqlserver_conn):
    sqlserver_cursor = sqlserver_conn.cursor()
    sqlserver_cursor.execute("SELECT table_name FROM information_schema.tables")
    tables = sqlserver_cursor.fetchall()
    sqlserver_cursor.close()
    return [table[0].lower() for table in tables]

def create_table_in_postgres(postgres_conn, table_name, columns):
    pg_cursor = postgres_conn.cursor()
    column_definitions = []
    for column in columns:
        column_name, data_type, char_length = column
        if data_type == 'nvarchar' or data_type == 'varchar':
            postgres_data_type = f'VARCHAR({char_length})'
        elif data_type == 'integer' or data_type == 'int':
            postgres_data_type = 'INT'
        elif data_type == 'text':
            postgres_data_type = 'TEXT'
        elif data_type == 'timestamp with time zone' or data_type == 'datetimeoffset':
            postgres_data_type = 'TIMESTAMPTZ'
        elif data_type == 'timestamp without time zone' or data_type == 'datetime':
            postgres_data_type = 'TIMESTAMP'
        elif data_type == 'boolean' or data_type == 'bit':
            postgres_data_type = 'BOOLEAN'
        elif data_type == 'date':
            postgres_data_type = 'DATE'
        elif data_type == 'numeric' or data_type == 'decimal':
            postgres_data_type = 'NUMERIC'
        elif data_type == 'double precision' or data_type == 'float':
            postgres_data_type = 'DOUBLE PRECISION'
        elif data_type == 'real':
            postgres_data_type = 'REAL'
        elif data_type == 'smallint':
            postgres_data_type = 'SMALLINT'
        elif data_type == 'bigint':
            postgres_data_type = 'BIGINT'
        elif data_type == 'money':
            postgres_data_type = 'MONEY'
        elif data_type == 'uniqueidentifier':
            postgres_data_type = 'UUID'
        # Add other data type mappings if needed
        else:
            postgres_data_type = data_type.upper()

        column_definitions.append(f"{column_name} {postgres_data_type}")

    create_table_query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"
    pg_cursor.execute(create_table_query)
    pg_cursor.close()

def copy_data(sqlserver_conn, postgres_conn, table_name):
    sqlserver_cursor = sqlserver_conn.cursor()
    pg_cursor = postgres_conn.cursor()
    
    sqlserver_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlserver_cursor.fetchall()
    
    for row in rows:
        placeholders = ','.join(['%s'] * len(row))
        pg_cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
        
    postgres_conn.commit()
    sqlserver_cursor.close()
    pg_cursor.close()

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

if __name__ == "__main__":
    main()
