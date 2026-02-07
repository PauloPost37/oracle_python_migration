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
        # Group columns by index_name to handle composite indexes correctly
        indexes_map = {}
        for index in column_data_dict[table]["indexes"]:
            # Correct Mapping based on extract_data.py
            # 0: index_name, 1: column_name, 2: column_position, ..., 6: table_owner, 7: table_name
            
            index_name = index[0]
            column_name = index[1]
            column_pos = index[2]
            schema_name = index[6]
            table_name = index[7]
            uniqueness = index[5] # Not used in CREATE INDEX, usually handled by constraints
            
            if index_name not in indexes_map:
                indexes_map[index_name] = {
                    "schema": schema_name,
                    "table": table_name,
                    "columns": []
                }
            
            indexes_map[index_name]["columns"].append((column_pos, column_name))
        
        # Generate CREATE INDEX statements
        for index_name, details in indexes_map.items():
            # Sort columns by position
            details["columns"].sort(key=lambda x: x[0])
            
            # Create column list string: "col1", "col2"
            column_list_str = ", ".join([f'"{col[1].lower()}"' for col in details["columns"]])
            
            # Format index name: idx_unique_col or similar
            # Use lowercase for standard postgres names
            pg_index_name = f"{index_name.lower()}"
            
            schema_name = details["schema"]
            table_name = details["table"]
            
            statement = f"""CREATE INDEX "{pg_index_name}" ON "{schema_name}"."{table_name.lower()}" ({column_list_str});\n"""
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
            f'    "pw" : "{oracle_data["pw"]}",\n'
            f'    "host" : "{oracle_data.get("host", "")}",\n'
            f'    "port" : "{oracle_data.get("port", "")}",\n'
            f'    "sid" : "{oracle_data.get("sid", "")}",\n'
            f'    "use_sid" : {str(oracle_data.get("use_sid", False)).lower().capitalize()}\n'
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


def configure_postgreSQL():
    print(f"The sytem has {os.cpu_count()} amount of cores")
    ram_bytes = psutil.virtual_memory().total
    ram_mb = ram_bytes/1024/1024
    print(f"The sytem has {ram_bytes/1024/1024} MB of  Ram")

    if (ram_mb * 0.4 * 1024) < 128:
        shared_buffer = 128
    shared_buffer = int(ram_mb * 0.4)
    effective_cache_size = int(ram_mb * 0.5)
    maintenance_work_mem = "128"
    if shared_buffer/32 < 16:
        wal_buffers = shared_buffer/32
    else:
        wal_buffers = "16"
    default_statistics_target = 100
    #pg_cursor.execute(f"ALTER SYSTEM SET shared_buffer = '{shared_buffer} MB'")
    #pg_cursor.execute(f"ALTER SYSTEM SET effective_cache_size = '{effective_cache_size} MB'")
    #pg_cursor.execute(f"ALTER SYSTEM SET maintenance_work_mem = '{maintenance_work_mem}'")
    #pg_cursor.execute(f"ALTER SYSTEM SET wal_buffers = '{wal_buffers} MB'")
    #pg_cursor.execute(f"ALTER SYSTEM SET default_statistics_target = '{default_statistics_target}'")
    list_of_configs = [shared_buffer, effective_cache_size,maintenance_work_mem, wal_buffers, default_statistics_target]
    return list_of_configs


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
        
        dsn = oracle_conf["cs"]
        if oracle_conf.get("use_sid"):
             dsn = oracledb.makedsn(oracle_conf["host"], oracle_conf["port"], sid=oracle_conf["sid"])

        connection_oracle = establish_oracle_connection(oracle_conf["un"], oracle_conf["pw"], dsn)
        
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
            start_migration = time.time()
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
        start_data = time.time()
        row_count = 0
        for table in tables:
            row_count += column_data_dict[table]["row_count"]
        pg_insert.migrate_data(connection_oracle, conn_pg, schema, tables, column_data_dict, pg_conf, oracle_conf, row_count, os.cpu_count())
        end_data = time.time()
        total_time_data = end_data - start_data
        yield f"Data migration finished. {total_time_data} seconds with {row_count} total rows\n"
        
        
        yield "Step 3: Creating Sequences (sequences.txt)...\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "sequences.txt")
        sequence_errors = migration_errors
        
        
        yield "Step 4: Creating Views (view.txt)...\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "view.txt")
        view_errors = migration_errors
        
        yield "Step 5: Applying Constraints & Indexes (output_alter.txt)\n"
        migration_errors = 0
        execute_sql_file(conn_pg, "output_alter.txt")
        ci_errors = migration_errors

        end_migration = time.time()
        total_migration_time = end_migration - start_migration
        yield "Migration completed successfully!\n"
        pg_server_config = configure_postgreSQL()
        yield f"Table Migration Errors: {table_errors}\n"
        yield f"Sequence Migration Errors: {sequence_errors}\n"
        yield f"View Migration Errors: {view_errors}\n"
        yield f"Constraint and Index Migration errors: {ci_errors}\n"
        yield f"Total migration time: {total_migration_time}, Data Migration time: {total_time_data} \n"
        yield f"Check the migration_report.txt for a full report\n"

        with open("migration_report.txt", "a") as f:
            f.write("Migration completed. For optimized PostgreSQL configurations change the following parameters in the postgresql.conf: \n")
            f.write(f"Set: 'shared_buffer' to '{pg_server_config[0]} MB'\n")
            f.write(f"Set: 'effective_cache_size' to '{pg_server_config[1]} MB'\n")
            f.write(f"Set: 'maintenance work memory' to '{pg_server_config[2]} KB'\n")
            f.write(f"Set: 'wal_buffer' to '{pg_server_config[3]} MB'\n")
            f.write(f"Set: 'default_statistics_target' to '{pg_server_config[4]}'\n")



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
        # Determine DSN
        dsn = oracle_conf["cs"]
        if oracle_conf.get("use_sid"):
            dsn = oracledb.makedsn(oracle_conf["host"], oracle_conf["port"], sid=oracle_conf["sid"])

        conn = establish_oracle_connection(oracle_conf["un"], oracle_conf["pw"], dsn)
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
        if sys.argv[0]:
            print("Okay it works")

        # Start browser in a separate thread
        threading.Thread(target=open_browser).start()
        # Run server
        app.run(debug=False, port=5000, host='0.0.0.0')




