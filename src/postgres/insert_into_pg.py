import psycopg2.extras
from psycopg2 import pool
from multiprocessing import Pool

import psycopg2
from psycopg2 import pool
from multiprocessing import Process
import os
import time
import oracledb
import time


def responsible_tables(column_data_dict, tables, row_count, processes):

    split_tables_list = []
    row_count_per_process = int(row_count/processes)
    local_row_count = 0
    current_counter = 0
    for p in range(processes):
        split_tables_list.append([])


    for table in tables:
        row_count = column_data_dict[table]["row_count"]
        if (row_count + local_row_count) > row_count_per_process and current_counter < processes -1 :
            current_counter += 1
            local_row_count = 0

        split_tables_list[current_counter].append(table)
        local_row_count += row_count

    return tuple(split_tables_list)




# Mit Gemini 3 pro generiert
def normalize_row(row):
    """
    Helper to convert various Oracle data types to Python native types 
    that psycopg2 can understand. Specifically handles LOBs.
    """
    new_row = []
    for item in row:
        # Handle Oracle LOB objects (BLOB/CLOB) - they have a read() method
        if item and hasattr(item, 'read'):
            try:
                # Read the entire content of the LOB
                new_row.append(item.read())
            except Exception:
                # Fallback if read fails, though it shouldn't for valid LOBs
                new_row.append(str(item))
        else:
            new_row.append(item)
    return tuple(new_row)


# Generated using Gemini 3 pro 
def migrate_parralell(table_set, connection_data_pg, connection_data_oracle, column_data_dict,schema, batch_size=2000):
    pg_conn = psycopg2.connect(f"dbname={connection_data_pg["database_name"]} user={connection_data_pg["user"]} password={connection_data_pg["password"]} host={connection_data_pg["host"]} port={connection_data_pg["port"]}")

    oracle_conn = oracledb.connect(user=connection_data_oracle["un"], password=connection_data_oracle["pw"], dsn=connection_data_oracle["cs"])
    pg_cursor = pg_conn.cursor()

    time.sleep(2)

    start = time.time()
    for table in table_set:
        with open("migration_report.txt", "a") as f:
            f.write(f"Migrating {table} with {column_data_dict[table]["row_count"]}       ")
        # 1. Build SQL Strings
        cols = [row[0] for row in column_data_dict[table]["columns"]]
        quoted_cols = ", ".join(f'"{c.lower()}"' for c in cols)
        quoted_cols_ora = ", ".join(f'"{c}"' for c in cols) # Oracle needs uppercase
        placeholders = ", ".join(["%s"] * len(cols))

        insert_sql = f'INSERT INTO "{schema}"."{table.lower()}" ({quoted_cols}) VALUES ({placeholders})'
        select_sql = f'SELECT {quoted_cols_ora} FROM "{schema}"."{table}"'
            
        # 2. Stream Data from Oracle
        with oracle_conn.cursor() as ora_cursor:
            ora_cursor.arraysize = batch_size
            ora_cursor.execute(select_sql)
                
            total_rows = 0
            while True:
                # Fetch a batch of rows
                rows = ora_cursor.fetchmany(batch_size)
                if not rows:
                    break
                    
                # Normalize types (handle BLOBs, CLOBs, etc)
                cleaned_rows = [normalize_row(row) for row in rows]
                    
                # 3. Batch Insert into Postgres
                try:
                    psycopg2.extras.execute_batch(pg_cursor, insert_sql, cleaned_rows, page_size=batch_size)
                    total_rows += len(rows)
                except Exception as e:
                    pg_conn.rollback()
                    print(e)
                    raise e
                        
            # Commit after each table is fully migrated
        pg_conn.commit()

        pg_cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table.lower()}"')
        row_count_table = pg_cursor.fetchone()

        with open("migration_report.txt", "a") as f:
            f.write(f"Migrated {table} with {row_count_table[0]}\n")
    end = time.time()
    total_time = end - start
    print(total_time)



def migrate_data_single(oracle_conn, pg_conn, schema, tables, column_data_dict, batch_size=2000):
    """
    Migrates data from Oracle to Postgres using server-side cursors and batch inserts.
    This is memory efficient and fast.
    """
    pg_cursor = pg_conn.cursor()

    for table in tables:
        # 1. Build SQL Strings
        cols = [row[0] for row in column_data_dict[table]["columns"]]
        quoted_cols = ", ".join(f'"{c.lower()}"' for c in cols)
        quoted_cols_ora = ", ".join(f'"{c}"' for c in cols) # Oracle needs uppercase
        placeholders = ", ".join(["%s"] * len(cols))
        
        insert_sql = f'INSERT INTO "{schema}"."{table.lower()}" ({quoted_cols}) VALUES ({placeholders})'
        select_sql = f'SELECT {quoted_cols_ora} FROM "{schema}"."{table}"'
        
        # 2. Stream Data from Oracle
        with oracle_conn.cursor() as ora_cursor:
            ora_cursor.arraysize = batch_size
            ora_cursor.execute(select_sql)

            total_rows = 0
            while True:
                # Fetch a batch of rows
                rows = ora_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Normalize types (handle BLOBs, CLOBs, etc)
                cleaned_rows = [normalize_row(row) for row in rows]
                
                # 3. Batch Insert into Postgres
                try:
                    psycopg2.extras.execute_batch(pg_cursor, insert_sql, cleaned_rows, page_size=batch_size)
                    total_rows += len(rows)
                except Exception as e:
                    pg_conn.rollback()
                    raise e
                    
        # Commit after each table is fully migrated
        pg_conn.commit()












# connection_oracle, conn_pg, schema, tables, column_data_dict, pg_conf, oracle_conf, row_count, os.cpu_count()
def migrate_data(oracle_conn, pg_conn, schema, tables, column_data_dict, connection_data_pg, connection_data_oracle, row_count, processors_count, batch_size=2000,):

    #thread_pool = pool.ThreadedConnectionPool(min_connection, max_connection, user=connection_data["user"], password=connection_data["password"], host=connection_data["host"], port=connection_data["port"], database=connection_data["database_name"]) 
    pg_cursor = pg_conn.cursor()
    
    split_tables_tuple = responsible_tables(column_data_dict, tables, row_count, processors_count)
    active_processes = []
    #migrate_data_single(oracle_conn, pg_conn, schema, tables, column_data_dict) 
    for table_set in split_tables_tuple:
        p = Process(target=migrate_parralell, args=(table_set, connection_data_pg, connection_data_oracle, column_data_dict,schema))
        p.start()
        active_processes.append(p)
    for p in active_processes:
        p.join()


