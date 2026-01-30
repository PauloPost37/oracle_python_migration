# Debugged with Chat gpt 5.2
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
                "condition" : constraint[11],
                "deferrable" : constraint[12],
                "deferred" : constraint[13],
                "validated" : constraint[15],
                "ref_owner" : constraint[6],
                "ref_constraint" : constraint[7],
                "ref_table" : constraint[8],
                "ref_columns" : [],
            }
            
            # Columns                     # Poistion
        if constraint[4] is not None and constraint[5] is not None:
            grouped_constraints[con_name]["columns"].append((constraint[5], constraint[4]))
        if constraint[9] is not None and constraint[10] is not None:
            grouped_constraints[con_name]["ref_columns"].append((constraint[10], constraint[9]))

    for con_name in grouped_constraints:
        grouped_constraints[con_name]["columns"].sort(key=lambda x:x[0])

    return grouped_constraints

# Debugged and heavily expanded with chat GPT
def create_postgreSQL_alter_DDL(schema, tables, column_data_dict):
    cleared = False
    for table in tables:
        if not cleared:
            open("output_alter.txt", "w").close()
            cleared = True

        grouped_constraints = group_constraints(column_data_dict[table]["constraints"])

        constraint_names = grouped_constraints.keys()
        #print(grouped_constraints)
        #print(constraint_names)
        for constraint in constraint_names:
            con_type = grouped_constraints[constraint]["con_type"]
            #print(con_type)
            if con_type == "P":
                continue
            if con_type == "C":
                con_condition = grouped_constraints[constraint]["condition"]
                if not con_condition:
                    continue
                if " IS NOT NULL" not in con_condition.upper():
                    alter_table_statement = f"""ALTER TABLE "{schema}"."{table.lower()}" """
                    alter_table_statement += f"""ADD CONSTRAINT "{constraint.lower()}" CHECK ({con_condition})"""

                    deferrable = grouped_constraints[constraint].get("deferrable")
                    deferred = grouped_constraints[constraint].get("deferred")
                    validated = grouped_constraints[constraint].get("validated")

                    if deferrable and deferrable.upper() == "DEFERRABLE":
                        alter_table_statement += " DEFERRABLE"
                        if deferred and deferred.upper() == "DEFERRED":
                            alter_table_statement += " INITIALLY DEFERRED"
                        else:
                            alter_table_statement += " INITIALLY IMMEDIATE"
                    else:
                        alter_table_statement += " NOT DEFERRABLE"

                    if validated and validated.upper() == "NOT VALIDATED":
                        alter_table_statement += " NOT VALIDATED"

                    alter_table_statement += ";\n"
                    with open("output_alter.txt", "a") as output:
                        output.write(alter_table_statement)

            if con_type == "U":
                alter_table_statement = f"""ALTER TABLE "{schema}"."{table.lower()}" """
                
                columns = ", ".join(f'"{col.lower()}"' for _, col in grouped_constraints[constraint]["columns"])

                alter_table_statement += f"""ADD CONSTRAINT "{constraint.lower()}" UNIQUE ({columns})"""

                deferrable = grouped_constraints[constraint].get("deferrable")
                deferred = grouped_constraints[constraint].get("deferred")
                validated = grouped_constraints[constraint].get("validated")

                if deferrable and deferrable.upper() == "DEFERRABLE":
                    alter_table_statement += " DEFERRABLE"
                    if deferred and deferred.upper() == "DEFERRED":
                        alter_table_statement += " INITIALLY DEFERRED"
                    else:
                        alter_table_statement += " INITIALLY IMMEDIATE"
                else:
                    alter_table_statement += " NOT DEFERRABLE"

                if validated and validated.upper() == "NOT VALIDATED":
                    alter_table_statement += " NOT VALIDATED"

                alter_table_statement += ";\n"
                with open("output_alter.txt", "a") as output:
                    output.write(alter_table_statement)

            if con_type == "R":
                child_cols = ", ".join(
                    f'"{col.lower()}"' for _, col in sorted(grouped_constraints[constraint]["columns"], key=lambda x: x[0])
                )
                parent_cols = ", ".join(
                    f'"{col.lower()}"' for _, col in sorted(grouped_constraints[constraint]["ref_columns"], key=lambda x: x[0])
                )

                ref_owner = grouped_constraints[constraint].get("ref_owner", schema)
                ref_table = grouped_constraints[constraint].get("ref_table", table)

                alter_table_statement = f"""ALTER TABLE "{schema}"."{table.lower()}" """
                alter_table_statement += f"""ADD CONSTRAINT "{constraint.lower()}" FOREIGN KEY ({child_cols}) REFERENCES "{ref_owner}"."{ref_table.lower()}" ({parent_cols})"""


                deferrable = grouped_constraints[constraint].get("deferrable")
                deferred = grouped_constraints[constraint].get("deferred")
                validated = grouped_constraints[constraint].get("validated")

                if deferrable and deferrable.upper() == "DEFERRABLE":
                    alter_table_statement += " DEFERRABLE"
                    if deferred and deferred.upper() == "DEFERRED":
                        alter_table_statement += " INITIALLY DEFERRED"
                    else:
                        alter_table_statement += " INITIALLY IMMEDIATE"
                else:
                    alter_table_statement += " NOT DEFERRABLE"

                if validated and validated.upper() == "NOT VALIDATED":
                    alter_table_statement += " NOT VALIDATED"

                alter_table_statement += ";\n"
                with open("output_alter.txt", "a") as output:
                    output.write(alter_table_statement)


        # Handle unique indexes that are not represented as constraints
        unique_indexes = {}
        for index in column_data_dict[table]["indexes"]:
            index_name, column_name, column_position, descend, index_type, uniqueness, table_owner, table_name, table_type = index
            if uniqueness != "UNIQUE":
                continue
            unique_indexes.setdefault(index_name, []).append((column_position, column_name))

        for index_name, cols in unique_indexes.items():
            cols.sort(key=lambda x: x[0])
            col_list = ", ".join(f'"{col}"' for _, col in cols)
            create_index_stmt = f"""CREATE UNIQUE INDEX "{index_name}" ON "{schema}"."{table}" ({col_list});\n"""
            with open("output_alter.txt", "a") as output:
                output.write(create_index_stmt)
            
