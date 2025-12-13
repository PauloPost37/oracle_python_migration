import oracledb
import getpass
import psycopg2


## Oracle 
un = "DEMO_MIGRATION"
cs = "localhost/xepdb1"
pw = getpass.getpass(f"Enter password for {un}@{cs}: ")


## Postgres
database_name = "postgres"
user = "postgres"
password = "abcd1234"
host = "localhost"
port = "5432"



tables_info = {}

def establish_postgres_connection(database_name, user, password, host, port):
    connection = psycopg2.connect(f"dbname={database_name} user={user} password={password} host={host} port={port}")

    return connection



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
    "DATE" : "timestamp",
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
            column_constraint_sql = """SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner, cons.constraint_type
                                        FROM all_constraints cons, all_cons_columns cols
                                        WHERE cols.table_name = :t
                                        AND cons.constraint_name = cols.constraint_name
                                        AND cons.owner = cols.owner
                                        ORDER BY cols.table_name, cols.position"""
            
            index_sql = """
                        select ind.index_name,
                            ind_col.column_name,
                            ind.index_type,
                            ind.uniqueness,
                            ind.table_owner as schema_name,
                            ind.table_name as object_name,
                            ind.table_type as object_type       
                        from sys.all_indexes ind
                        inner join sys.all_ind_columns ind_col on ind.owner = ind_col.index_owner
                                                            and ind.index_name = ind_col.index_name
                        -- excluding some Oracle maintained schemas
                        where ind.owner not in ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS', 'LBACSYS', 
                        'MDSYS', 'MGMT_VIEW','OLAPSYS','OWBSYS','ORDPLUGINS', 'ORDSYS','OUTLN', 
                        'SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM', 'TSMSYS','WK_TEST',
                        'WKPROXY','WMSYS','XDB','APEX_040000', 'APEX_PUBLIC_USER','DIP', 'WKSYS',
                        'FLOWS_30000','FLOWS_FILES','MDDATA', 'ORACLE_OCM', 'XS$NULL',
                        'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'PUBLIC')
                        AND ind.table_name = :t
                        order by ind.table_owner,
                                ind.table_name,
                                ind.index_name,
                                ind_col.column_position
                        """

            cursor.execute(count_sql)
            row_count =  cursor.fetchone()[0]

            cursor.execute(column_data_sql, {"t":safe_table})
            column_data = cursor.fetchall()

            cursor.execute(column_constraint_sql, {"t":safe_table})
            column_constraint_data = cursor.fetchall()

            cursor.execute(index_sql, {"t":safe_table})
            index_data = cursor.fetchall()

            #print(column_constraint_data)




            column_data_dict[table] = {"row_count" : row_count, "columns" : [], "constraints" : [], "indexes": [], "foreign_keys": []}
            column_comment = get_column_comments(conn, table)
            #print(column_comment)

            
            for column_name, data_type, data_length, data_precision, data_scale, nullable in column_data:
                column_data_dict[table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale, nullable, None, None])
            for counter in range(len(column_comment)):
                column_data_dict[table]["columns"][counter][6] = column_comment[counter][0]

            #print(column_data_dict)

            for j in range (len(column_data_dict[table]["columns"])):
                for i in range (len(column_constraint_data)):
                    if column_data_dict[table]["columns"][j][0] == column_constraint_data[i][1]:
                        print(column_constraint_data[i][5])
                        if column_constraint_data[i][5] == "P":
                            column_data_dict[table]["columns"][j][7] = "Primary"
                        if column_constraint_data[i][5] != "P" and column_constraint_data[i][5] != "C":
                            column_data_dict[table]["constraints"].append(column_constraint_data[i])


            column_data_dict[table]["indexes"]= index_data
    #print(column_data_dict)
    return column_data_dict




def create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping):
    schema_creation_sql = f"CREATE SCHEMA IF NOT EXISTS {un.lower()};"
    create_tables_sql = ""
    create_tables_comment_sql = ""
    oracle_specefic_names = ["select", "from"]
    for table in tables:
        data_for_table_dict = column_data_dict.get(table)
        create_tables_sql += f"""CREATE TABLE "{un.lower()}"."{table}" (\n"""
        column_data_list = data_for_table_dict.get("columns")
        set_primary_key = ""
        for column in column_data_list:
            column_name = column[0]
            correct_mapping = data_mapping.get(column[1])
            if column[5] == "N":
                create_tables_sql += f""""{column_name}"      {correct_mapping} NOT NULL,\n"""
            else:
                create_tables_sql += f""""{column_name}"      {correct_mapping},\n"""
            if column[-2] != None:
                create_tables_comment_sql += f"""comment on column "{un.lower()}"."{table}"."{column_name}" is '{column[-2]}';\n"""
            
            if column[7] == "Primary":
                set_primary_key = f"""PRIMARY KEY ("{column_name}")\n"""
        create_tables_sql += set_primary_key
        create_tables_sql += ");\n"
    print(schema_creation_sql)
    print(create_tables_sql)
    print(create_tables_comment_sql)

    return schema_creation_sql, create_tables_sql, create_tables_comment_sql

def get_oracle_data(connection, tables):
    column_data = {}
    for table in tables:
        cursor = connection.cursor()
        select_sql = f"""SELECT * FROM "{table}" """
        cursor.execute(select_sql)
        column_data_tuple = cursor.fetchall()
        column_data[table] = column_data_tuple 
    return(column_data)
    


def clean_oracle_data(oracle_data_sql):
    oracle_data_sql = oracle_data_sql
    oracle_data_sql_key_list = oracle_data_sql.keys()
    cleaned_dict = {}
    for key in oracle_data_sql_key_list:
        clean_rows_list = []
        for row in oracle_data_sql[key]:
            clean_row_list = []
            for data in range(len(row)):
                if isinstance(row[data], oracledb.LOB):
                    clean_row_list.append(row[data].read())
                else:
                    clean_row_list.append(row[data])
            clean_rows_list.append(clean_row_list)
        cleaned_dict[key] = clean_rows_list
    return cleaned_dict


def create_insert_ddl(cleaned_data_dict):
    #cleaned_data_keys = cleaned_data_dict.keys()
    insert_sql = ""
    cleaned_data_keys = ["DM_CUSTOMER_SIMPLE"]
    for key in cleaned_data_keys:
        for row in cleaned_data_dict[key]:
            insert_row_sql = f"""INSERT INTO "{key}" ("ID", "NAME", "EMAIL", "STATUS", "CREATED_AT", "NOTES")
            ("""
            for element in row:
                if row[-1] == element:
                    insert_row_sql += f"""'{element}')"""
                else:
                    insert_row_sql += f"""'{element}',"""
            print(insert_row_sql)







def create_postgreSQL_Schema(connection, schema_ddl):
    cursor = connection.cursor()
    cursor.execute(f"{schema_ddl}")
    connection.commit()

def create_postgreSQL_table(connection, table_ddl):
    cursor = connection.cursor()
    cursor.execute(f"{table_ddl}")
    connection.commit()

def create_postgreSQL_comments(connection, comment_ddl):
    cursor = connection.cursor()
    cursor.execute(f"{comment_ddl}")
    connection.commit()

def main():
    connection_oracle = establish_oracle_connection(un, pw, cs)
    connection_postgres = establish_postgres_connection(database_name, user, password, host, port)


    tables = extract_tables(connection_oracle)
    column_data_dict = extract_column_data(tables, connection_oracle)
    #create_schema_sql, create_tables_sql, create_tables_comments_sql = create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping)
    oracle_data_sql = get_oracle_data(connection_oracle, tables)

    #create_postgreSQL_Schema(connection_postgres, create_schema_sql)
    #create_postgreSQL_table(connection_postgres, create_tables_sql)
    #create_postgreSQL_table(connection_postgres, create_tables_comments_sql)


    cleaned_data = clean_oracle_data(oracle_data_sql)
    

    create_insert_ddl(cleaned_data)

        
main()



