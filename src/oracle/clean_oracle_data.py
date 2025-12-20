import oracledb

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


