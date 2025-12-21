import oracledb
import psycopg2
import pprint
from config.config import data_mapping, oracle_connection_data, postgres_connection_data
from src.oracle.connection_oracle import establish_oracle_connection
from src.postgres.conection_postgres import establish_postgres_connection
import src.oracle.extract_data as oracle_extract


import src.postgres.create as pg_create
import src.postgres.insert_into_pg as pg_insert







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


def create_postgres_indexes(column_data_dict):
    tables = column_data_dict.keys()
    insert_index_statements = []

    for table in tables:
        for index in column_data_dict[table]["indexes"]:
            if index[3] == "UNIQUE":
                index_name = f"idx_{index[5]}_{index[1]}"
                statement = f"""CREATE UNIQUE INDEX "{index_name}" ON "{index[4]}"."{index[5]}" ("{index[1]}")"""
                insert_index_statements.append(statement)
            else:
                index_name = f"idx_{index[5]}_{index[1]}"
                statement = f"""CREATE INDEX "{index_name}" ON "{index[4]}"."{index[5]}" ("{index[1]}")"""
                insert_index_statements.append(statement)
    return insert_index_statements
            
def main():
    connection_oracle = establish_oracle_connection(oracle_connection_data["un"], oracle_connection_data["pw"], oracle_connection_data["cs"])
    connection_postgres = establish_postgres_connection(postgres_connection_data["database_name"], postgres_connection_data["user"], postgres_connection_data["password"], postgres_connection_data["host"], postgres_connection_data["port"])

    schemas = oracle_extract.get_all_schemas(connection_oracle)
    schemas = ['DEMO_MIGRATION']
    for schema in schemas:
    # List of all Tables
        tables = oracle_extract.get_tables(connection_oracle, schema)


        # Creates a dictionary with all relevant data for each table: {"table": {"row_count" : int, "columns" : [], "constraints" : [], "indexes": [], "foreign_keys": []}}
        column_data_dict = oracle_extract.create_data_dict(tables)

        
        column_data_dict = oracle_extract.get_column_row_count(connection_oracle, column_data_dict, schema)
        column_data_dict = oracle_extract.get_column_constraints(connection_oracle, column_data_dict, schema)
        column_data_dict = oracle_extract.get_column_data(connection_oracle, column_data_dict, schema)
        column_data_dict = oracle_extract.get_oracle_indexes(connection_oracle, column_data_dict, schema)
        # # Creates Schmeas, tables and comments
        #create_schema_sql, table_ddls, comment_ddls = pg_create.create_postgreSQL_DDL(schema, tables, column_data_dict, data_mapping)


        #print(create_schema_sql, table_ddls, comment_ddls)
        #remove_primary_indexes(column_data_dict)
        pprint.pprint(column_data_dict)
        #index_dll = create_postgres_indexes(column_data_dict)
        pg_create.create_postgreSQL_DDL(schema, tables, column_data_dict, data_mapping)
        #print(index_dll)
        # create_postgreSQL_Schema(connection_postgres, create_schema_sql)
        # exec_pg_list(connection_postgres, table_ddls)
        # exec_pg_list(connection_postgres, comment_ddls)



        # # Gets oracle Data
        # oracle_data_sql = get_oracle_data(connection_oracle, tables, schema)
        # # Cleans oracle Data so it can be inserted
        # cleaned_data = clean_oracle_data(oracle_data_sql)
        

        # insert_data(cleaned_data, connection_postgres, column_data_dict, tables, schema)
        
main()

## TODO ##

# correctly translate bytea

## Mit dem DDL vom Backup vergleichen beim create

## Backup erstellen lassen und parsen 

## PL/SQL Trigger anschauen

