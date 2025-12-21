"""
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
                                        cons.validated,
                                        cons.INDEX_NAME
"""

def group_constraints(constraint_rows):

    grouped_constraints = {}

    for constraint in constraint_rows:
        con_name = constraint[2]

        if con_name not in grouped_constraints:
            grouped_constraints[con_name] = {
                "con_type" : constraint[3],
                "schema" : constraint[0],
                "table" : constraint[1],
                "columns" : [],
                "condition" : constraint[8]
                                          }
            
            # Columns                     # Poistion
        if constraint[4] is not None and constraint[5] is not None:
            grouped_constraints[con_name]["columns"].append((constraint[5], constraint[4]))

    for con_name in grouped_constraints:
        grouped_constraints[con_name]["columns"].sort(key=lambda x:x[0])

    return grouped_constraints

def create_postgreSQL_alter_DDL(schema, tables, column_data_dict):
    for table in tables:
        grouped_constraints = group_constraints(column_data_dict[table]["constraints"])

        constraint_names = grouped_constraints.keys()
        print(grouped_constraints)
        print(constraint_names)
        for constraint in constraint_names:
            con_type = grouped_constraints[constraint]["con_type"]
            print(con_type)
            if con_type == "P":
                continue
            if con_type == "C":
                con_condition = grouped_constraints[constraint]["condition"]
                if " IS NOT NULL" not in con_condition.upper():
                    alter_table_statement = f"""ALTER TABLE "{schema}"."{table}"\n"""
                    alter_table_statement += f"""ADD CONSTRAINT "{constraint}" CHECK ({con_condition});\n"""
                    with open("output_alter.txt", "a") as output:
                        output.write(alter_table_statement)

                if not con_condition:
                    continue

            if con_type == "U":
                alter_table_statement = f"""ALTER TABLE "{schema}"."{table}"\n"""
                
                # Warum so:: columns = ", ".join(f'"{col}"' for _, col in grouped_constraints[constraint]["columns"])
                columns = ", ".join(f'"{col}"' for _, col in grouped_constraints[constraint]["columns"])

                #columns = ", ".join(f'"{c[0]}"' for c in grouped_constraints[constraint]["columns"]) 
                alter_table_statement += f"""ADD CONSTRAINT "{constraint}" UNIQUE ({columns});\n"""
                with open("output_alter.txt", "a") as output:
                    output.write(alter_table_statement)
                
            # if con_type == "R":
            #     alter_table_statement = f"""ALTER TABLE "{schema}"."{table}"\n"""
            #     columns = ", ".join(f'"{c}"' for c in group_constraints[constraint]["columns"]) 
            #     alter_table_statement += f"""ADD CONSTRAINT "{constraint}" UNIQUE ({columns});"""



## ToDo 

# Foreign keys hinzufügen
# Nicht alle alterstatments seperat in die Datei schreiben