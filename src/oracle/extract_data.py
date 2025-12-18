import oracledb

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

def get_tables(conn, owner):

    with conn.cursor() as cursor:
        sql_tables = "SELECT table_name FROM all_tables WHERE owner = :t"
        tables = []
        for r in cursor.execute(sql_tables, {"t":owner}):
            tables.append(r[0])
    return tables

def get_column_data(tables, conn, schema):
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