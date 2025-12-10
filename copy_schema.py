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


def get_column_comments(conn, table):
    with conn.cursor() as cursor:
        column_comments_sql = "SELECT comments FROM user_col_comments WHERE table_name = :t"
        cursor.execute(column_comments_sql, {"t":table})
        column_comments = cursor.fetchall()
        return column_comments
            

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
            column_data_sql = f"SELECT column_name, data_type, data_length, data_precision, data_scale, nullable FROM all_tab_columns where table_name = :t"
            column_constraint_sql = """SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
                                        FROM all_constraints cons, all_cons_columns cols
                                        WHERE cols.table_name = :t
                                        AND cons.constraint_type = 'P'
                                        AND cons.constraint_name = cols.constraint_name
                                        AND cons.owner = cols.owner
                                        ORDER BY cols.table_name, cols.position"""

            cursor.execute(count_sql)
            row_count =  cursor.fetchone()[0]

            cursor.execute(column_data_sql, {"t":safe_table})
            column_data = cursor.fetchall()

            cursor.execute(column_constraint_sql, {"t":safe_table})
            column_constraint_data = cursor.fetchall()
            print(column_constraint_data)




            column_data_dict[table] = {"row_count" : row_count, "columns" : []}
            column_comment = get_column_comments(conn, table)
            #print(column_comment)

            
            for column_name, data_type, data_length, data_precision, data_scale, nullable in column_data:
                column_data_dict[table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale, nullable])
            for counter in range(len(column_comment)):
                column_data_dict[table]["columns"][counter].append(column_comment[counter][0])

            print(column_data_dict)

            for j in range (len(column_data_dict[table]["columns"])):
                if column_data_dict[table]["columns"][j][0] == column_constraint_data[0][1]:
                    column_data_dict[table]["columns"][j].append("Primary")
                else:
                    column_data_dict[table]["columns"][j].append(None)

    #print(column_data_dict)
    return column_data_dict




def create_postgreSQL_schema(un, tables, column_data_dict, data_mapping):
    schema_creation_sql = f"CREATE SCHEMA AUTHORIZATION {un}"
    create_tables_sql = ""
    create_tables_comment_sql = ""
    for table in tables:
        data_for_table_dict = column_data_dict.get(table)
        create_tables_sql += f"CREATE TABLE {table} (\n"
        column_data_list = data_for_table_dict.get("columns")
        for column in column_data_list:
            correct_mapping = data_mapping.get(column[1])
            if column == column_data_list[-1]:
                if column[5] == "N":
                    create_tables_sql += f"{column[0]}      {correct_mapping} NOT NULL\n"
                else:
                    create_tables_sql += f"{column[0]}      {correct_mapping}\n"
            else:
                if column[5] == "N":
                    create_tables_sql += f"{column[0]}      {correct_mapping} NOT NULL,\n"
                else:
                    create_tables_sql += f"{column[0]}      {correct_mapping},\n"
            if column[-2] != None:
                create_tables_comment_sql += f"comment on column {table}.{column[0]} is '{column[-2]}';\n"
            

        create_tables_sql += ");\n"
    print(schema_creation_sql)
    print(create_tables_sql)
    print(create_tables_comment_sql)

def main():
    connection = establish_oracle_connection(un, pw, cs)
    tables = extract_tables(connection)
    column_data_dict = extract_column_data(tables, connection)
    create_postgreSQL_schema(un, tables, column_data_dict, data_mapping)

        
main()


