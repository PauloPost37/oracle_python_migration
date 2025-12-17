import oracledb
import getpass
import psycopg2
import pprint

## Oracle Connection Data
un = "ADMIN_ALL"
cs = "localhost/xepdb1"
pw = getpass.getpass(f"Enter password for {un}@{cs}: ")


## Postgres Connection Data
database_name = "postgres"
user = "postgres"
password = "abcd1234"
host = "localhost"
port = "5432"



def establish_postgres_connection(database_name, user, password, host, port):
    connection = psycopg2.connect(f"dbname={database_name} user={user} password={password} host={host} port={port}")

    return connection

def establish_oracle_connection(un, pw, cs):
    try: 
        connection = oracledb.connect(user=un, password=pw, dsn=cs)
        return connection
    except oracledb.DatabaseError as e:
        print(f"Error connecting to oracle Database: {e}")

# ORA2PG Data Mapping
data_mapping = {
    "NUMBER" : "numeric",
    "CHAR" : "char",
    "NCHAR" : "char",
    "VARCHAR" : "text",  ## Hier könnte es probleme geben
    "NVARCHAR" : "varchar",
    "VARCHAR2" : "text",  ## Unterschiede Varchar2 und Varchar bei oracle - 
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


def get_column_comments(conn, table, schema):
    with conn.cursor() as cursor:
        column_comments_sql = f"SELECT comments FROM all_col_comments WHERE owner = '{schema}' AND table_name = :t "
        cursor.execute(column_comments_sql, {"t": table})
        column_comments = cursor.fetchall()
        return column_comments


def get_all_schemas(conn):
    oracle_schemas = ["ADMIN_ALL","APPQOSSYS","ANONYMOUS","AUDSYS", "CTXSYS", "DBSFWUSER", "DBSNMP","DGPDB_INT","DIP","DVF", "DVSYS","GGSYS","GSMCATUSER","GSMUSER","MDDATA","ORACLE_OCM","ORDPLUGINS","PDBADMIN","REMOTE_SCHEDULER_AGENT","SI_INFORMTN_SCHEMA","SYSBACKUP","SYSDG","SYSKM","SYSRAC","SYS$UMF","XS$NULL", "GSMADMIN_INTERNAL", "LBACSYS", "MDSYS", "OJVMSYS", "OLAPSYS","ORDDATA", "ORDSYS","OUTLN", "SYS", "SYSTEM", "WMSYS", "XDB"]
    with conn.cursor() as cursor:
        sql_schemata = "SELECT username FROM all_users ORDER BY username"
        schemata = []
        for r in cursor.execute(sql_schemata):
            if r[0] not in oracle_schemas:
                schemata.append(r[0])
    return schemata

def extract_tables(conn, owner):

    with conn.cursor() as cursor:
        sql_tables = "SELECT table_name FROM all_tables WHERE owner = :t"
        tables = []
        for r in cursor.execute(sql_tables, {"t":owner}):
            tables.append(r[0])
    return tables

def extract_column_data(tables, conn, schema):
    # Dict stores table as key, values are another dictionary which stores the row_count: int and columns : []
    column_data_dict = {}
    with conn.cursor() as cursor:
        for table in tables:
            safe_table = table.replace('"', '""')
            count_sql = f""" SELECT COUNT(*) FROM {schema}."{table}" """
            #https://stackoverflow.com/questions/22962114/get-data-type-of-field-in-select-statement-in-oracle
            # Selects general data about table information
            column_data_sql = f"SELECT column_name, data_type, data_length, data_precision, data_scale, nullable FROM all_tab_columns where table_name = :t"

            # Selects the constraints of each table
            column_constraint_sql = """SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner, cons.constraint_type, cons.constraint_name
                                        FROM all_constraints cons, all_cons_columns cols
                                        WHERE cols.table_name = :t
                                        AND cons.constraint_name = cols.constraint_name
                                        AND cons.owner = cols.owner
                                        ORDER BY cols.table_name, cols.position"""
            
            # Selects all Indexes of a given table
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

            ############# Executes all of the sql statements ##################
            cursor.execute(count_sql)
            row_count =  cursor.fetchone()[0]

            cursor.execute(column_data_sql, {"t":safe_table})
            column_data = cursor.fetchall()

            cursor.execute(column_constraint_sql, {"t":safe_table})
            column_constraint_data = cursor.fetchall()

            cursor.execute(index_sql, {"t":safe_table})
            index_data = cursor.fetchall()

            ######################################################################




            column_data_dict[table] = {"row_count" : row_count, "columns" : [], "constraints" : [], "indexes": [], "foreign_keys": []}

            # gets the comments of the columns for a specific table
            column_comment = get_column_comments(conn, table, schema)

            # Cycles through the column data and appends it to the columns value list
            for column_name, data_type, data_length, data_precision, data_scale, nullable in column_data:
                column_data_dict[table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale, nullable, None, None])

            # Does the same for the comments
            for counter in range(len(column_comment)):
                column_data_dict[table]["columns"][counter][6] = column_comment[counter][0]

            #print(column_data_dict)

            # Inserts the constraints into the dictionary
            for j in range (len(column_data_dict[table]["columns"])):
                for i in range (len(column_constraint_data)):
                    if column_data_dict[table]["columns"][j][0] == column_constraint_data[i][1]:
                        if column_constraint_data[i][5] == "P":
                            column_data_dict[table]["columns"][j][7] = "Primary"
                            column_data_dict[table]["constraints"].append(column_constraint_data[i])
                        
                        column_data_dict[table]["constraints"].append(column_constraint_data[i])
                        


            column_data_dict[table]["indexes"]= index_data
    #print(column_data_dict)
    return column_data_dict


### Broken ###


# Function to generate the postgresDDL for Schema, Table and comments
# def create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping):
#     schema_creation_sql = f"CREATE SCHEMA IF NOT EXISTS {un.lower()};"
#     create_tables_sql = ""
#     # Chatgpt fix #
#     create_tables_comment_sql = []
#     # #
#     oracle_specefic_names = ["select", "from"]
#     for table in tables:
#         data_for_table_dict = column_data_dict.get(table)
#         create_tables_sql += f"""CREATE TABLE IF NOT EXISTS "{un.lower()}"."{table}" (\n"""
#         column_data_list = data_for_table_dict.get("columns")
#         set_primary_key = ""
#         for column in column_data_list:
#             column_name = column[0]
#             correct_mapping = data_mapping.get(column[1])
#             if column[5] == "N":
#                 create_tables_sql += f""""{column_name}"      {correct_mapping} NOT NULL,\n"""
#             else:
#                 create_tables_sql += f""""{column_name}"      {correct_mapping},\n"""
#             if column[-2] is not None:
#                 # GPT fix #
#                 comment_text = str(column[-2]).replace("'", "''")  # escape single quotes for PG
#                 create_tables_comment_sql.append(
#                     f'COMMENT ON COLUMN "{un.lower()}"."{table}"."{column_name}" IS \'{comment_text}\';'
#                 )


#                 ### ###
#                 """
#                 create_tables_comment_sql += fcomment on column "{un.lower()}"."{table}"."{column_name}" is '{column[-2]}';\n

#                 """
#             if column[7] == "Primary":
#                 set_primary_key = f"""PRIMARY KEY ("{column_name}")\n"""
#         create_tables_sql += set_primary_key
#         create_tables_sql += ");\n"
#     #print(schema_creation_sql)
#     #print(create_tables_sql)
#     #print(create_tables_comment_sql)

#     return schema_creation_sql, create_tables_sql, create_tables_comment_sql

####

def create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping):
    schema_creation_sql = f'CREATE SCHEMA IF NOT EXISTS "{un.lower()}";'

    table_ddls = []          # list of CREATE TABLE statements
    comment_ddls = []        # list of COMMENT statements

    for table in tables:
        data_for_table_dict = column_data_dict[table]
        column_data_list = data_for_table_dict["columns"]

        col_lines = []
        pk_cols = []

        for column in column_data_list:
            column_name = column[0]
            oracle_type = column[1]
            correct_mapping = data_mapping.get(oracle_type, "text")  # default text

            nullable = column[5]  # "N" means NOT NULL
            not_null = " NOT NULL" if nullable == "N" else ""

            col_lines.append(f'"{column_name}" {correct_mapping}{not_null}')

            # comments (escape quotes for PG)
            if column[6] is not None:
                comment_text = str(column[6]).replace("'", "''")
                comment_ddls.append(
                    f'COMMENT ON COLUMN "{un.lower()}"."{table}"."{column_name}" IS \'{comment_text}\';'
                )

            if column[7] == "Primary":
                pk_cols.append(column_name)

        if pk_cols:
            col_lines.append('PRIMARY KEY (' + ", ".join(f'"{c}"' for c in pk_cols) + ')')

        ddl = f'CREATE TABLE IF NOT EXISTS "{un.lower()}"."{table}" (\n  ' + ",\n  ".join(col_lines) + "\n);"
        table_ddls.append(ddl)

    return schema_creation_sql, table_ddls, comment_ddls




def get_oracle_data(connection, tables, schema):
    column_data = {}
    for table in tables:
        cursor = connection.cursor()
        select_sql = f"""SELECT * FROM "{schema}"."{table}" """
        cursor.execute(select_sql)
        column_data_tuple = cursor.fetchall()
        column_data[table] = column_data_tuple 
    return(column_data)
    

# Replaces LOBS with strings 
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
            clean_rows_list.append(tuple(clean_row_list))
        cleaned_dict[key] = clean_rows_list
    return cleaned_dict

# Doesnt actually create a DDL needs to be refactored 
def create_insert_ddl(cleaned_data_dict, connection_postgres, column_data_dict, tables, schema):
    # Saves all column names for the insert statement
    table_build = {}
    # Saves the amount of columns for the Insert statement
    table_col_nums = {}
    for table in tables:
        
        cols = [row[0] for row in column_data_dict[table]["columns"]]
 
        ## Chatgpt 5.2 ##
        col_sql = ", ".join(f'"{c}"' for c in cols)
        ##             ##

        table_col_nums[table] = len(cols)
        table_build[table] = col_sql
    
    cursor = connection_postgres.cursor()
    for key in tables:
        for row in cleaned_data_dict[key]:
            
            ## Chatgpt 5.2 ##
            placeholder = ", ".join(["%s"] * table_col_nums[key])
            ##             ##

            insert_row_sql = f"""INSERT INTO {schema}."{key}" ({table_build[key]})
             VALUES ({placeholder});"""

            cursor.execute(insert_row_sql, row)
            connection_postgres.commit()


def create_postgreSQL_Schema(connection, schema_ddl):
    cursor = connection.cursor()
    cursor.execute(f"{schema_ddl}")
    connection.commit()

"""
### BROKEN
def create_postgreSQL_table(connection, table_ddl):
    cursor = connection.cursor()
    print(table_ddl)
    cursor.execute(f"{table_ddl}")
    connection.commit()

def create_postgreSQL_comments(connection, comment_ddl):
    cursor = connection.cursor()
    cursor.execute(f"{comment_ddl}")
    connection.commit()
"""

### Chatgpt 5.2 fix

def exec_pg_list(connection, stmts):
    cur = connection.cursor()
    for stmt in stmts:
        cur.execute(stmt)
    connection.commit()

 
###################

def remove_primary_indexes(column_data_dict):
    tables = column_data_dict.keys()
    primary_key_constraint_names = []
    for table in tables:
        for constraint in column_data_dict[table]["constraints"]:
            if constraint[5] == "P":
                # Adds the constraint name to the list if the typ is P (Primary Key)
                primary_key_constraint_names.append(constraint[6])
        
    for table in tables:
        x = len(column_data_dict[table]["indexes"])
        for index in column_data_dict[table]["indexes"]:
            if index[0] in primary_key_constraint_names:
                column_data_dict[table]["indexes"].remove(index)

    return column_data_dict

def main():
    # Connections to oracle and postgres
    connection_oracle = establish_oracle_connection(un, pw, cs)
    connection_postgres = establish_postgres_connection(database_name, user, password, host, port)

    schemas = get_all_schemas(connection_oracle)

    for schema in schemas:
    # List of all Tables
        tables = extract_tables(connection_oracle, schema)
    #print(tables)

        # Create the dict with all Table information
        column_data_dict = extract_column_data(tables, connection_oracle, schema)

        #print(column_data_dict)


        # Creates Schmeas, tables and comments
        create_schema_sql, table_ddls, comment_ddls = create_postgreSQL_DDL(schema, tables, column_data_dict, data_mapping)

        create_postgreSQL_Schema(connection_postgres, create_schema_sql)
        exec_pg_list(connection_postgres, table_ddls)
        exec_pg_list(connection_postgres, comment_ddls)



        # Gets oracle Data
        oracle_data_sql = get_oracle_data(connection_oracle, tables, schema)
        # Cleans oracle Data so it can be inserted
        cleaned_data = clean_oracle_data(oracle_data_sql)
        

        create_insert_ddl(cleaned_data, connection_postgres, column_data_dict, tables, schema)


def debug_main():
    connection_oracle = establish_oracle_connection(un, pw, cs)
    connection_postgres = establish_postgres_connection(database_name, user, password, host, port)

    schemas = get_all_schemas(connection_oracle)

    for schema in schemas:
    # List of all Tables
        tables = extract_tables(connection_oracle, schema)
    #print(tables)

        # Create the dict with all Table information
        column_data_dict = extract_column_data(tables, connection_oracle, schema)

        print("")
        pprint.pprint(column_data_dict)
        remove_primary_indexes(column_data_dict)
        print("")
        pprint.pprint(column_data_dict)


        # # Creates Schmeas, tables and comments
        # create_schema_sql, table_ddls, comment_ddls = create_postgreSQL_DDL(schema, tables, column_data_dict, data_mapping)

        # create_postgreSQL_Schema(connection_postgres, create_schema_sql)
        # exec_pg_list(connection_postgres, table_ddls)
        # exec_pg_list(connection_postgres, comment_ddls)



        # # Gets oracle Data
        # oracle_data_sql = get_oracle_data(connection_oracle, tables, schema)
        # # Cleans oracle Data so it can be inserted
        # cleaned_data = clean_oracle_data(oracle_data_sql)
        

        # create_insert_ddl(cleaned_data, connection_postgres, column_data_dict, tables, schema)
        
debug_main()

## TODO ##

# correctly translate bytea

## Mit dem DDL vom Backup vergleichen beim create

## Backup erstellen lassen und parsen 

## PL/SQL Trigger anschauen

