import psycopg2

def establish_postgres_connection(database_name, user, password, host, port):
    connection = psycopg2.connect(f"dbname={database_name} user={user} password={password} host={host} port={port}")

    return connection
