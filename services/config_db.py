import sqlite3 as lite


def insert_db_config(db_type, host, port, user, password, database):
    con = lite.connect('config.db')
    with con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config_db (
                db_type TEXT, 
                host TEXT, 
                port TEXT, 
                user TEXT, 
                password TEXT, 
                database TEXT, 
                UNIQUE(db_type, host, IFNULL(port, ''), user, password, database)
            )
        """)
        if db_type == 'sqlserver':
            port = ''  # Set port to empty string for sqlserver
        cur.execute("SELECT * FROM config_db WHERE db_type=? AND host=? AND IFNULL(port, '')=? AND user=? AND password=? AND database=?",
                    (db_type, host, port, user, password, database))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO config_db VALUES(?, ?, ?, ?, ?, ?)",
                        (db_type, host, port, user, password, database))
            con.commit()
    con.close()


def retrieve_all_type_db_config():
    con = lite.connect('config.db')
    print("Select Database Configuration")
    with con:
        cur = con.cursor()
        cur.execute("SELECT db_type FROM config_db")
        rows = cur.fetchall()
        for row in rows:
            print(f"DB Type: {row[0]}")
    con.close()


def retrieve_db_config_db_type(db_type):
    con = lite.connect('config.db')
    print(f"Select Database Configuration for {db_type}")
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM config_db WHERE db_type=?", (db_type,))
        rows = cur.fetchall()
        for row in rows:
            print(f"DB Type: {row[0]}, Host: {row[1]}, Port: {row[2]}, User: {
                  row[3]}, Password: {row[4]}, Database: {row[5]}")

# def main():
#     retrieve_all_type_db_config()
#     while True:
#         db_type = input("Enter the database type: ")
#         print("\n" + "="*40 + "\n")
#         retrieve_db_config_db_type(db_type)
#         continue_input = input("Do you want to continue? (y/n): ").strip().lower()
#         if continue_input != 'y':
#             break
#     print('Have a good day, goodbye!')

# if __name__ == "__main__":
#     main()
