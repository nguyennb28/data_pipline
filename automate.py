import services.config_db as config_db
import services.connect_db as connect_db
import services.sync_data as sync_data
import schedule
import time
import functools

# Show all database configuration type
def get_list_db_type():
    list_db_type = config_db.get_all_db_type()
    return list_db_type


# -----------------------
def get_db_config_by_db_type(list_db_type):
    db_config = []
    for db_type in list_db_type:
        db_config.append(config_db.get_db_config_by_db_type(db_type))
    return db_config
    
# -----------------------
# Connect database from config.db
def connect_db_by_info_db(info_db):
    if info_db['db_type'] == 'sqlserver':
        sqlserver_conn = connect_db.get_sqlserver_connection(
            info_db['host'],
            info_db['database'],
            info_db['user'],
            info_db['password']
        )
        return sqlserver_conn
    elif info_db['db_type'] == 'postgres':
        postgres_conn = connect_db.get_postgres_connection(
            info_db['host'],
            info_db['port'],
            info_db['database'],
            info_db['user'],
            info_db['password']
        )
        return postgres_conn

# -----------------------
# Check data if not exist -> insert data
def update_data(mssql_conn, pg_conn, tables_sqlserver):
    for table in tables_sqlserver:
        check_table = connect_db.table_exists_in_pg(pg_conn, table)
        if check_table is not True:
            # create table
            sync_data.create_table_in_postgres(pg_conn, table, connect_db.get_table_structure(mssql_conn, table))
            # copy data
            sync_data.copy_data(mssql_conn, pg_conn, table)
            print(f"Table {table} is created")
            print(f"Data from {table} is copied")
        else:            
            # check data is exists
            latest_record_id_pg = sync_data.get_latest_record_id_from_postgres(pg_conn, table)
            if (sync_data.check_new_records_in_sqlserver(mssql_conn, table, latest_record_id_pg)):
            #copy data
                sync_data.copy_data_with_id(mssql_conn, pg_conn, table, latest_record_id_pg)
                print(f"Data from {table} is updated")
    
# Setup schedule to sync data

def main():
    # Show all record database configuration
    config_db.retrieve_all_type_db_config()
    
    list_db_type = get_list_db_type()
    # Info database configuration
    info_dbs = get_db_config_by_db_type(list_db_type)
    # Connect database and sync data
    mssql_conn = None
    pg_conn = None
    for info_db in info_dbs:
        if info_db['db_type'] == 'sqlserver':
            mssql_conn = connect_db_by_info_db(info_db)
        elif info_db['db_type'] == 'postgres':
            pg_conn = connect_db_by_info_db(info_db)
    
    # List table from sqlserver
    tables_sqlserver = sync_data.list_tables_in_sqlserver(mssql_conn)
    # for table in tables_sqlserver:
    #     check_table = connect_db.table_exists_in_pg(pg_conn, table)
    #     if check_table is not True:
    #         # create table
    #         sync_data.create_table_in_postgres(pg_conn, table, connect_db.get_table_structure(mssql_conn, table))
    #         # copy data
    #         sync_data.copy_data(mssql_conn, pg_conn, table)
    #     else:            
    #         # check data is exists
    #         latest_record_id_pg = sync_data.get_latest_record_id_from_postgres(pg_conn, table)
    #         if (sync_data.check_new_records_in_sqlserver(mssql_conn, table, latest_record_id_pg)):
    #         #copy data
    #             sync_data.copy_data_with_id(mssql_conn, pg_conn, table, latest_record_id_pg)
    #             print(f"Data from {table} is updated")
    
    update_data(mssql_conn, pg_conn, tables_sqlserver)

if __name__ == "__main__":
    main()