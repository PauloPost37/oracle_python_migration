import oracledb
import psycopg2
import pprint
from config.config import data_mapping, oracle_connection_data, postgres_connection_data
from src.oracle.connection_oracle import establish_oracle_connection
from src.postgres.conection_postgres import establish_postgres_connection
import src.oracle.extract_data as oracle_extract
import src.postgres.create as pg_create
import src.postgres.insert_into_pg as pg_insert
import src.postgres.alter_table as alter_table
import logging
import time 

# Edited by Gemini 3 pro
import sys
import os
import threading
import webbrowser
import time
import re
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import psutil

app = Flask(__name__)

logger = logging.getLogger("migration_tool")
logger.setLevel(logging.INFO)

info_logger = logging.FileHandler("migration_info.log", encoding="utf-8")
info_logger.setLevel(logging.INFO)

error_logger = logging.FileHandler("migration_errors.log", encoding="utf-8")
error_logger.setLevel(logging.ERROR)

## ChatGPT 5.2
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
info_logger.setFormatter(formatter)
error_logger.setFormatter(formatter)

logger.addHandler(info_logger)
logger.addHandler(error_logger)
##
migration_errors = 0

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

# Debugged with Gemini 3 pro
def create_postgres_indexes(column_data_dict):
    tables = column_data_dict.keys()
    insert_index_statements = []

    for table in tables:
        for index in column_data_dict[table]["indexes"]:
            # Correct Mapping based on extract_data.py
            # 0: index_name, 1: column_name, ..., 6: table_owner, 7: table_name
            
            index_name = index[0]
            column_name = index[1]
            schema_name = index[6]
            table_name = index[7]
            uniqueness = index[5]
            
            # Format index name: idx_unique_col or similar
            # Use lowercase for standard postgres names
            pg_index_name = f"{index_name.lower()}"
            
            statement = f"""CREATE INDEX "{pg_index_name}" ON "{schema_name}"."{table_name.lower()}" ("{column_name.lower()}")\n"""
            with open("output_alter.txt", "a") as output:
                output.write(statement)

    return insert_index_statements
            
# Added by Gemini 3 pro
def update_config_file(oracle_data, pg_data):
    """Updates the config.py file with new connection details."""
    try:
        config_path = os.path.join("config", "config.py")
        with open(config_path, "r") as f:
            content = f.read()

        oracle_str = (
            'oracle_connection_data = {\n'
            f'    "un" : "{oracle_data["un"]}",\n'
            f'    "cs" : "{oracle_data["cs"]}",\n'
            f'    "pw" : "{oracle_data["pw"]}"\n'
            '}'
        )

        pg_str = (
            'postgres_connection_data = {\n'
            f'    "database_name" : "{pg_data["database_name"]}",\n'
            f'    "user" : "{pg_data["user"]}",\n'
            f'    "password" : "{pg_data["password"]}",\n'
            f'    "host" : "{pg_data["host"]}",\n'
            f'    "port" : "{pg_data["port"]}"\n'
            '}'
        )
        
        # Replace using regex to match the dictionary blocks
        content = re.sub(r'oracle_connection_data\s*=\s*\{[^}]+\}', oracle_str, content, flags=re.MULTILINE|re.DOTALL)
        content = re.sub(r'postgres_connection_data\s*=\s*\{[^}]+\}', pg_str, content, flags=re.MULTILINE|re.DOTALL)

        with open(config_path, "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Failed to update config file: {e}")

def execute_sql_file(conn, file_path):
    global migration_errors
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                for sql_line in f:
                        if sql_line.strip() and sql_line != "":
                            logger.info(sql_line)
                            try:
                                with conn.cursor() as cur:
                                    cur.execute(sql_line)
                                    conn.commit()
                            except Exception as e:
                                logger.error(sql_line)
                                logger.error(e)
                                migration_errors += 1
                                conn.rollback()
                return True
    except Exception as e:
        logger.warning(e)
    return False


def configure_postgreSQL(pg_conn):
    #psycopg2.connect(pg_conn)
    pg_cursor = pg_conn.cursor()
    print(f"The sytem has {os.cpu_count()} amount of cores")
    ram_bytes = psutil.virtual_memory().total
    ram_mb = ram_bytes/1024
    print(f"The sytem has {ram_bytes/1024/1024} GB of  Ram")

    shared_buffer = int(ram_mb * 0.4)
    effective_cache_size = int(ram_mb * 0.5)
    maintenance_work_mem = "128 MB"
    if shared_buffer/32 < 16:
        wal_buffers = shared_buffer/32
    else:
        wal_buffers = "16 MB"
    default_statistics_target = 100

    print(f"This results in a shared buffer of: {shared_buffer} MB")
    print(f"This results in an effective cache size of {effective_cache_size} MB")
    print(f"This results in a maintenance work memory of {maintenance_work_mem}")
    print(f"This results in a wal buffer of {wal_buffers} MB")
    print(f"This results in a default_statistics_ {default_statistics_target}")


# Edited (old main()) and adjusted by Gemini 3 pro to work with webUI
def run_migration_task(schemas, oracle_conf, pg_conf):

    global migration_errors
    """Generator function that runs the migration logic and yields log messages."""
    yield "Starting migration process...\\n"
    
    conn_pg = None
    try:
        # clear all output files at start
        open("output.txt", "w").close()
        open("output_oneline.txt", "w").close()
        open("output_alter.txt", "w").close()
        open("sequences.txt", "w").close()
        open("view.txt", "w").close()

        yield "Connecting to Oracle database...\\n"
        connection_oracle = establish_oracle_connection(oracle_conf["un"], oracle_conf["pw"], oracle_conf["cs"])
        
        yield "Connecting to PostgreSQL database...\\n"
        conn_pg = establish_postgres_connection(
            pg_conf["database_name"],
            pg_conf["user"], 
            pg_conf["password"],
            pg_conf["host"],
            pg_conf["port"]
        )

        for schema in schemas:
            yield f"Processing schema: {schema}\\n"
            
            # List of all Tables
            tables = oracle_extract.get_tables(connection_oracle, schema)
            yield f"Found {len(tables)} tables in schema {schema}.\\n"

            # Creates a dictionary with all relevant data
            column_data_dict = oracle_extract.create_data_dict(tables)

            yield "Extracting column details...\n"
            column_data_dict = oracle_extract.get_column_row_count(connection_oracle, column_data_dict, schema)
            column_data_dict = oracle_extract.get_column_constraints(connection_oracle, column_data_dict, schema)
            column_data_dict = oracle_extract.get_column_data(connection_oracle, column_data_dict, schema)
            column_data_dict = oracle_extract.get_oracle_indexes(connection_oracle, column_data_dict, schema)
            
            yield "Extracting views and sequences...\n"
            oracle_extract.get_oracle_views(connection_oracle, schema)
            sequences = oracle_extract.get_oracle_sequences(connection_oracle, schema)
            
            yield "Generating PostgreSQL DDL...\n"
            pg_create.create_postgreSQL_DDL(schema, tables, column_data_dict, data_mapping)
            pg_create.create_postgreSQL_Sequences(sequences, schema)
            
            yield "Generating Alter Table statements...\n"
            alter_table.create_postgreSQL_alter_DDL(schema, tables, column_data_dict)
            
            yield "Generating Indexes...\n"
            create_postgres_indexes(column_data_dict)
            
            yield f"Completed schema extraction for {schema}.\n"

        yield "Executing generated SQL on PostgreSQL...\n"
        
        yield "Step 1: Creating Tables (output.txt)...\n"
        execute_sql_file(conn_pg, "output_oneline.txt")
        table_errors = migration_errors
        
        yield "Step 2: Migrating Data (streaming rows)...\n"
        start = time.time()
        row_count = 0
        for table in tables:
            row_count += column_data_dict[table]["row_count"]
        pg_insert.migrate_data(connection_oracle, conn_pg, schema, tables, column_data_dict, pg_conf, oracle_conf, row_count, os.cpu_count())
        end = time.time()
        total_time = end - start
        yield f"Data migration finished. {total_time} seconds with {row_count} total rows\n"
        
        
        yield "Step 3: Creating Sequences (sequences.txt)...\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "sequences.txt")
        sequence_errors = migration_errors
        
        
        yield "Step 4: Creating Views (view.txt)...\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "view.txt")
        view_errors = migration_errors
        
        yield "Step 5: Applying Constraints & Indexes (output_alter.txt) testo...\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "output_alter.txt")
        ci_errors = migration_errors

        yield "Migration completed successfully!\n"
        configure_postgreSQL(conn_pg)

        yield f"Table Migration Errors: {table_errors}\n"
        yield f"Sequence Migration Errors: {sequence_errors}\n"
        yield f"View Migration Errors: {view_errors}\n"
        yield f"Constraint and Index Migration errors: {ci_errors}\n"
    except Exception as e:
        yield f"ERROR: {str(e)}\\n"
        import traceback
        yield traceback.format_exc()
    finally:
        if conn_pg:
            conn_pg.close()


# Gemini 3 pro
@app.route('/')
def index():
    return render_template('index.html', oracle=oracle_connection_data, postgres=postgres_connection_data)


# Das kann ich nochmal selbst schreiben
# Gemini 3 pro
@app.route('/get_schemas', methods=['POST'])
def get_schemas():
    data = request.json
    oracle_conf = data.get('oracle')
    pg_conf = data.get('postgres')
    
    try:
        conn = establish_oracle_connection(oracle_conf["un"], oracle_conf["pw"], oracle_conf["cs"])
        schemas = oracle_extract.get_all_schemas(conn)
        conn.close()
        
        # Save working config
        update_config_file(oracle_conf, pg_conf)
        
        return jsonify(schemas=schemas)
    except Exception as e:
        return jsonify(error=str(e)), 500

# Gemini 3 pro
@app.route('/run_migration', methods=['POST'])
def run_migration():
    data = request.json
    schemas = data.get('schemas')
    oracle_conf = data.get('oracle')
    pg_conf = data.get('postgres')
    
    return Response(stream_with_context(run_migration_task(schemas, oracle_conf, pg_conf)), mimetype='text/plain')

# Gemini 3 pro
def open_browser():
    time.sleep(1.5)
    print("Opening web interface in browser...")
    webbrowser.open('http://127.0.0.1:5000')

if __name__ == '__main__':
    user_input = input("Welcome to the Oracle to PostgreSQL migration tool! This tool comes with multiple options to guide you through the migration process. Enter 1 for CLI and two for WebUI: ")
    if user_input == "1":
        print("The Migration through the CLI will use the config file placed under config/config.py.\n To ensure the connection works, please enter connection information before starting the migration process.\n Is the config.py configured correctly? [Y/N] : ")
    if user_input == "2":

        # Start browser in a separate thread
        threading.Thread(target=open_browser).start()
        # Run server
        app.run(debug=False, port=5000)

## TODO ##

# correctly translate bytea

