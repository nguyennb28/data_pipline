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
    
def copy_data_with_id(sqlserver_conn, postgres_conn, table_name, latest_id_pg):
    sqlserver_cursor = sqlserver_conn.cursor()
    pg_cursor = postgres_conn.cursor()
    
    sqlserver_cursor.execute(f"SELECT * FROM {table_name} WHERE id > %s", (latest_id_pg,))
    rows = sqlserver_cursor.fetchall()
    
    for row in rows:
        placeholders = ','.join(['%s'] * len(row))
        pg_cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
        
    postgres_conn.commit()
    sqlserver_cursor.close()
    pg_cursor.close()
    
def get_latest_record_id_from_postgres(pg_conn, table_name):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT MAX(id) FROM {table_name}")
    record_id = pg_cursor.fetchone()[0]
    pg_cursor.close()
    return record_id

def check_new_records_in_sqlserver(sqlserver_conn, table_name, latest_id_pg):
    sqlserver_cursor = sqlserver_conn.cursor()
    sqlserver_cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE id > %s", (latest_id_pg,))
    new_records_count = sqlserver_cursor.fetchone()[0]
    sqlserver_cursor.close()
    return new_records_count > 0
