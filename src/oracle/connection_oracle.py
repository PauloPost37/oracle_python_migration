import oracledb

def establish_oracle_connection(un, pw, cs):
    try: 
        connection = oracledb.connect(user=un, password=pw, dsn=cs)
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error connecting to oracle Database: {e}")
