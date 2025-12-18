# Doesnt actually create a DDL needs to be refactored 
def insert_data(cleaned_data_dict, connection_postgres, column_data_dict, tables, schema):
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