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


