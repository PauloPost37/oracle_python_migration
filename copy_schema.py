import oracledb
import getpass

un = "DEMO_MIGRATION"
cs = "localhost/xepdb1"
pw = getpass.getpass(f"Enter password for {un}@{cs}: ")

tables_info = {}

# ORA2PG Data Mapping
data_mapping = {
    "NUMBER" : "numeric",
    "CHAR" : "char",
    "NCHAR" : "char",
    "VARCHAR" : "varchar",
    "NVARCHAR" : "varchar",
    "VARCHAR2" : "varchar",
    "NVARCHAR" : "varchar",
    "NVARCHAR2" : "varchar",
    "STRING" : "varchar",
    "DATE" : "varchar",
    "LONG" : "text",
    "LONG RAW" : "bytes",
    "CLOB" : "text",
    "NCLOB" : "text",
    "BLOB" : "bytea",
    "BFILE" : "bytea",
    "RAW(16)" : "uuid",
    "RAW(32)" : "uuid", 
    "RAW" : "bytea",
    "ROWID" : "oid",
    "UROWID" : "oid", 
    "FLOAT" : "double precision",
    "DEC" : "decimal",
    "DECIMAL" : "decimal",
    "DOUBLE PRECISION" : "double precision", 
    "INT" : "integer",
    "INTEGER" : "integer", 
    "BINARY_INTEGER" : "integer",
    "PLS_INTEGER" : "integer", 
    "SMALLINT" : "smallint",
    "REAL" : "real", 
    "BINARY_FLOAT" : "numeric", 
    "BINARY_DOUBLE" : "numeric", 
    "TIMESTAMP" : "timestamp", 
    "BOOLEAN" : "boolean",
    "INTERVAL" : "interval", 
    "XMLTYPE" : "xml", 
    "TIMESTAMP WITH TIME ZONE" : "timestamp with time zone", 
    "TIMESTAMP WITH LOCAL TIME ZONE" : "timestamp with time zone",
    "SDO_GEOMETRY" : "geometry",
    "ST_GEOMETRY" : "geometry"
}


def establish_oracle_connection(un, pw, cs):
    try: 
        connection = oracledb.connect(user=un, password=pw, dsn=cs)
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error connecting to oracle Database: {e}")


def extract_tables(conn):
    with conn.cursor() as cursor:
        sql_tables = "SELECT table_name FROM user_tables ORDER BY table_name"
        tables = []
        for r in cursor.execute(sql_tables):
            tables.append(r[0])
    return tables

def extract_column_data(tables, conn):
    column_data_dict = {}
    with conn.cursor() as cursor:
        for table in tables:
            safe_table = table.replace('"', '""')
            count_sql = f'SELECT COUNT(*) FROM "{safe_table}"'
            #https://stackoverflow.com/questions/22962114/get-data-type-of-field-in-select-statement-in-oracle
            column_data_sql = f"SELECT column_name, data_type, data_length, data_precision, data_scale FROM all_tab_columns where table_name = :t"

            cursor.execute(count_sql)
            row_count =  cursor.fetchone()[0]

            cursor.execute(column_data_sql, {"t":safe_table})
            column_data = cursor.fetchall()
            column_data_dict[safe_table] = {"row_count" : row_count, "columns" : []}


            for column_name, data_type, data_length, data_precision, data_scale in column_data:
                column_data_dict[safe_table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale])


    #print(column_data_dict)
    return column_data_dict


def create_postgreSQL_schema(un, tables, column_data_dict, data_mapping):
    schema_creation_sql = f"CREATE SCHEMA AUTHORIZATION {un}"
    create_tables_sql = ""
    for table in tables:
        data_for_table_dict = column_data_dict.get(table)
        create_tables_sql += f"CREATE TABLE {table} (\n"
        column_data_list = data_for_table_dict.get("columns")
        for column in column_data_list:
            correct_mapping = data_mapping.get(column[1])
            if column == column_data_list[-1]:
                create_tables_sql += f"{column[0]}      {correct_mapping}\n"
            else:
                create_tables_sql += f"{column[0]}      {correct_mapping},\n"

        create_tables_sql += ");\n"
    print(create_tables_sql)

def main():
    connection = establish_oracle_connection(un, pw, cs)
    tables = extract_tables(connection)
    column_data_dict = extract_column_data(tables, connection)
    create_postgreSQL_schema(un, tables, column_data_dict, data_mapping)

        
main()






# with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
#     with connection.cursor() as cursor:
#         # 1) Tabellen des Users holen
#         sql_tables = "SELECT table_name FROM user_tables ORDER BY table_name"
#         print(f"Following tables were found in the Oracle Database for schema '{un}':")
#         tables = [r[0] for r in cursor.execute(sql_tables)]

#         for table in tables:
#             print(f"- {table}")
#             try:
#                 # 2) Rowcount
#                 safe_table = table.replace('"', '""')
#                 count_sql = f'SELECT COUNT(*) FROM "{safe_table}"'
#                 cursor.execute(count_sql)
#                 row_count = cursor.fetchone()[0]

#                 # 3) Spalteninformationen
#                 cols_sql = """
#                     SELECT column_name,
#                            data_type,
#                            data_length,
#                            data_precision,
#                            data_scale,
#                            nullable
#                     FROM user_tab_columns
#                     WHERE table_name = :t
#                     ORDER BY column_id
#                 """
#                 cursor.execute(cols_sql, {"t": table.upper()})
#                 columns = cursor.fetchall()  # Liste von Tupeln

#                 # Optional: schöner strukturieren
#                 columns_structured = [
#                     {
#                         "column_name": col_name,
#                         "data_type": data_type,
#                         "data_length": data_length,
#                         "data_precision": data_precision,
#                         "data_scale": data_scale,
#                         "nullable": nullable,
#                     }
#                     for (
#                         col_name,
#                         data_type,
#                         data_length,
#                         data_precision,
#                         data_scale,
#                         nullable,
#                     ) in columns
#                 ]

#                 tables_info[table] = {
#                     "row_count": row_count,
#                     "columns": columns_structured,
#                 }

#             except oracledb.DatabaseError as e:
#                 print(f"⚠️ Error processing table {table}: {e}")
#                 tables_info[table] = None

# print(tables_info)
