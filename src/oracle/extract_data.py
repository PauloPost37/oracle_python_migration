"""
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
"""

# Mit Chatgpt generiert bzw anpassen lassen
def get_column_comments(conn, table, schema, column_data_dict):
    with conn.cursor() as cursor:
        sql = """
            SELECT column_name, comments
            FROM all_col_comments
            WHERE owner = :owner
              AND table_name = :table_name
        """
        cursor.execute(sql, {"owner": schema, "table_name": table})
        rows = cursor.fetchall()

    # Build two maps: exact + upper() fallback
    comment_by_exact = {}
    comment_by_upper = {}
    for col_name, comment in rows:
        if col_name is None:
            continue
        comment_by_exact[col_name] = comment
        comment_by_upper[str(col_name).upper()] = comment

    # 2) Attach to your existing column list by matching names
    for col in column_data_dict[table]["columns"]:
        col_name = col[0]
        if col_name is None:
            continue

        # Try exact match first (handles quoted/mixed-case), then UPPER fallback
        comment = comment_by_exact.get(col_name)
        if comment is None:
            comment = comment_by_upper.get(str(col_name).upper())

        col[6] = comment  # may stay None if no comment exists

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

def   get_column_constraints(conn, column_data_dict, schema):
    tables = column_data_dict.keys()
    for table in tables:
        # SQL expanded by ChatGPT to encompass all constraints
        column_constraint_sql = """ SELECT
                                        cons.owner,
                                        cons.table_name,
                                        cons.constraint_name,
                                        cons.constraint_type,
                                        cols.column_name,
                                        cols.position,
                                        cons.r_owner,
                                        cons.r_constraint_name,
                                        rcols.table_name AS r_table_name,
                                        rcols.column_name AS r_column_name,
                                        rcols.position AS r_position,
                                        cons.search_condition,
                                        cons.deferrable,
                                        cons.deferred,
                                        cons.status,
                                        cons.validated,
                                        cons.INDEX_NAME
                                    FROM all_constraints cons
                                    LEFT JOIN all_cons_columns cols
                                        ON cons.owner = cols.owner
                                        AND cons.constraint_name = cols.constraint_name
                                        AND cons.table_name = cols.table_name
                                    LEFT JOIN all_cons_columns rcols
                                        ON cons.r_owner = rcols.owner
                                        AND cons.r_constraint_name = rcols.constraint_name
                                        AND rcols.position = cols.position
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
                search_condition = constraint[11]
                if search_condition != None:
                    search_condition = search_condition.upper().replace('"', '').strip()
                    if 'IS NOT NULL' not in search_condition:
                        cleaned_constraint_list.append(constraint)
                else:
                    cleaned_constraint_list.append(constraint)


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
                   nullable,
                   data_default,
                   identity_column
            FROM all_tab_columns
            WHERE table_name = :t
              AND owner = :s
            ORDER BY column_id
        """
        with conn.cursor() as cursor:
            
            cursor.execute(column_data_sql, {"t":table, "s": schema})
            column_data = cursor.fetchall()

        for column_name, data_type, data_length, data_precision, data_scale, nullable, data_default, identity_column in column_data:
                column_data_dict[table]["columns"].append([column_name, data_type, data_length, data_precision, data_scale, nullable, None, data_default, identity_column])

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


def get_oracle_indexes(conn, column_data_dict, schema):
    tables = column_data_dict.keys()

    index_sql = """
        select ind.index_name,
               ind_col.column_name,
               ind_col.column_position,
               ind_col.descend,
               ind.index_type,
               ind.uniqueness,
               ind.table_owner as schema_name,
               ind.table_name as object_name,
               ind.table_type as object_type
        from sys.all_indexes ind
        join sys.all_ind_columns ind_col
          on ind.owner = ind_col.index_owner
         and ind.index_name = ind_col.index_name
        where ind.owner not in (
            'ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','LBACSYS','MDSYS','MGMT_VIEW','OLAPSYS',
            'OWBSYS','ORDPLUGINS','ORDSYS','OUTLN','SI_INFORMTN_SCHEMA','SYS','SYSMAN',
            'SYSTEM','TSMSYS','WK_TEST','WKPROXY','WMSYS','XDB','APEX_040000',
            'APEX_PUBLIC_USER','DIP','WKSYS','FLOWS_30000','FLOWS_FILES','MDDATA',
            'ORACLE_OCM','XS$NULL','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','PUBLIC'
        )
          and ind.table_owner = :s
          and ind.table_name = :t
        order by ind.table_owner, ind.table_name, ind.index_name, ind_col.column_position
    """

    # Durch chatgpt angepasst
    for table in tables:
        # alle Index-Namen, die zu Constraints gehören (PK/UK etc.)
        constraint_index_names = {
            c[16] for c in column_data_dict[table]["constraints"] if c[16] is not None
        }

        #print(f"\n[{schema}.{table}]")
        #print("  constraint_index_names:", constraint_index_names)


        with conn.cursor() as cursor:
            cursor.execute(index_sql, {"t": table, "s": schema})
            indexes = cursor.fetchall()

        #print("  indexes_from_query:", {r[0] for r in indexes})  # r[0] == index_name
        #print("  indexes_kept:", {x[0] for x in column_data_dict[table]['indexes']})

        for (index_name, column_name, column_position, descend,
             index_type, uniqueness, table_owner, table_name, table_type) in indexes:

            # Nur "normale" Indexe, nicht die für Constraints
            if index_name not in constraint_index_names:
                column_data_dict[table]["indexes"].append([
                    index_name, column_name, column_position, descend,
                    index_type, uniqueness, table_owner, table_name, table_type
                ])

    return column_data_dict

