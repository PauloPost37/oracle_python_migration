def get_column_comments(conn, table, schema, column_data_dict):
    with conn.cursor() as cursor:
        column_comments_sql = f"SELECT comments FROM all_col_comments WHERE owner = '{schema}' AND table_name = :t "
        cursor.execute(column_comments_sql, {"t": table})
        column_comments = cursor.fetchall()

    # Attach comments to columns; guard against mismatch in counts
    for counter, comment in enumerate(column_comments):
        if counter < len(column_data_dict[table]["columns"]):
            column_data_dict[table]["columns"][counter][6] = comment[0]
    return column_data_dict


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

def create_data_dict(tables):
    column_data_dict = {}
    for table in tables:
        column_data_dict[table] = {"row_count" : int, "columns" : [], "constraints" : [], "indexes": [], "foreign_keys": []}
    return column_data_dict

def get_column_row_count(conn, column_data_dict, schema):
    tables = column_data_dict.keys()
    for table in tables:
        count_sql = f""" SELECT COUNT(*) FROM "{schema}"."{table}" """
        with conn.cursor() as cursor:
            
            cursor.execute(count_sql)
            row_count =  cursor.fetchone()[0]

        column_data_dict[table]["row_count"] = row_count
    return column_data_dict

def get_column_constraints(conn, column_data_dict, schema):
    tables = column_data_dict.keys()
    for table in tables:
        column_constraint_sql = """ SELECT
                                        cons.owner,
                                        cons.table_name,
                                        cons.constraint_name,
                                        cons.constraint_type,
                                        cols.column_name,
                                        cols.position,
                                        cons.r_owner,
                                        cons.r_constraint_name,
                                        cons.search_condition,
                                        cons.deferrable,
                                        cons.deferred,
                                        cons.status,
                                        cons.validated
                                    FROM all_constraints cons
                                    LEFT JOIN all_cons_columns cols
                                        ON cons.owner = cols.owner
                                        AND cons.constraint_name = cols.constraint_name
                                        AND cons.table_name = cols.table_name
                                    WHERE cons.owner = :s
                                    AND cons.table_name NOT LIKE 'BIN$%'
                                    AND cons.constraint_name NOT LIKE 'BIN$%'
                                    AND cons.table_name = :t
                                    ORDER BY
                                        cons.constraint_type,
                                        cons.constraint_name,
                                        cols.position
                                """
        with conn.cursor() as cursor:
            cursor.execute(column_constraint_sql, {"t":table, "s": schema})
            column_constraints = cursor.fetchall()
            #print(column_constraints)

            cleaned_constraint_list = []

            for constraint in column_constraints:
                search_condition = constraint[8]
                if search_condition != None:
                    search_condition = search_condition.upper().replace('"', '').strip()
                    if 'IS NOT NULL' not in search_condition:
                        cleaned_constraint_list.extend(constraint)
                else:
                    cleaned_constraint_list.extend(constraint)


            column_data_dict[table]["constraints"] = cleaned_constraint_list
    return column_data_dict

def get_column_data(conn, column_data_dict, schema):
    tables = column_data_dict.keys()
    for table in tables:
        column_data_sql = """
            SELECT column_name,
                   data_type,
                   data_length,
                   data_precision,
                   data_scale,
                   nullable
            FROM all_tab_columns
            WHERE table_name = :t
              AND owner = :s
            ORDER BY column_id
        """
        with conn.cursor() as cursor:
            
            cursor.execute(column_data_sql, {"t":table, "s": schema})
            column_data = cursor.fetchall()

        for column_name, data_type, data_length, data_precision, data_scale, nullable in column_data:
                column_data_dict[table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale, nullable, None, None])

        column_data_dict = get_column_comments(conn, table, schema, column_data_dict)

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

