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
# """
# def create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping):
#     schema_creation_sql = f'CREATE SCHEMA IF NOT EXISTS "{un}";'

#     table_ddls = []          # list of CREATE TABLE statements
#     comment_ddls = []        # list of COMMENT statements

#     for table in tables:
#         create_table_sql = ""

#         create_table_sql += """CREATE TABLE IF NOT EXISTS "{table}" ( \n"""

#         for column in column_data_dict[table]["columns"]:
#             column_name = column[0]
#             oracle_type = column[1]
#             correct_mapping = data_mapping.get(oracle_type, "text")  # default text

#             nullable = column[5]  # "N" means NOT NULL
#             not_null = " NOT NULL" if nullable == "N" else ""

#             create_table_sql += f'  "{column_name}"    {correct_mapping}{not_null}'

#             # comments (escape quotes for PG)
#             if column[6] is not None:
#                 comment_text = str(column[6]).replace("'", "''")
#                 comment_ddls.append(
#                     f'COMMENT ON COLUMN "{un.lower()}"."{table}"."{column_name}" IS \'{comment_text}\';'
#                 )

#             if column[7] == "Primary":
#                 pk_cols.append(column_name)

#         if pk_cols:
#             col_lines.append('PRIMARY KEY (' + ", ".join(f'"{c}"' for c in pk_cols) + ')')

#         ddl = f'CREATE TABLE IF NOT EXISTS "{un.lower()}"."{table}" (\n  ' + ",\n  ".join(col_lines) + "\n);"
#         table_ddls.append(ddl)

#     return schema_creation_sql, table_ddls, comment_ddls
# """

def group_primary_constraints(column_data_dict, table):
    pk_cols = []
    for c in column_data_dict[table]["constraints"]:
        if c[3] == "P":
            col_name = c[4]
            pos = c[5]
            pk_cols.append((pos, col_name))

    # nach position sortieren
    pk_cols.sort(key=lambda x: x[0])

    # nur Spaltennamen zurückgeben
    return [col for _, col in pk_cols]




def create_postgreSQL_DDL(un, tables, column_data_dict, data_mapping):
    schema_creation_sql = f'CREATE SCHEMA IF NOT EXISTS "{un}";\n'
    with open("output.txt", "a") as output:
        output.write(schema_creation_sql)
    for table in tables:
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS "{un}"."{table}" ( \n"""
        comments_sql = """"""
        for column in column_data_dict[table]["columns"]:
            column_name = column[0]
            oracle_type = column[1]
            nullable = column[5]  # "N" means NOT NULL
            oracle_default = column[7]
            oracle_identity = column[8]
            correct_mapping = data_mapping.get(oracle_type, "text")
 
            # ADD not_null constraint
            not_null = " NOT NULL" if nullable == "N" else ""

            # ADD Default
            pg_default = None
            if oracle_default:
                if oracle_default.strip().upper() == "SYSDATE":
                    pg_default = "CURRENT_TIMESTAMP"
                else:
                    pg_default = oracle_default

            default = f" DEFAULT {pg_default}" if pg_default else ""

            # Add identity

            identity_sql = ""
            if oracle_identity == "YES":
                identity_sql = " GENERATED BY DEFAULT AS IDENTITY"
                default = ""


            if column == column_data_dict[table]["columns"][-1]:

                cols = group_primary_constraints(column_data_dict, table)
                if cols:
                    create_table_sql += f'  "{column_name}"    {correct_mapping}{identity_sql}{not_null}{default},\n'
                    pk_sql = '  PRIMARY KEY (' + ', '.join(f'"{c}"' for c in cols) + ')\n'
                    create_table_sql += pk_sql
                else:
                    create_table_sql += f'  "{column_name}"    {correct_mapping}{identity_sql}{not_null}{default}\n'
            else:
                create_table_sql += f'  "{column_name}"    {correct_mapping}{identity_sql}{not_null}{default},\n'
        create_table_sql += ");\n\n"
        if column[6] is not None:
            comment_text = str(column[6]).replace("'", "''")
            comments_sql += f'COMMENT ON COLUMN "{un.lower()}"."{table}"."{column_name}" IS \'{comment_text}\';'
        with open("output.txt", "a") as output:
            output.write(create_table_sql)
            output.write(comments_sql)







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

def exec_pg_list(connection, stmts):
    cur = connection.cursor()
    for stmt in stmts:
        cur.execute(stmt)
    connection.commit()