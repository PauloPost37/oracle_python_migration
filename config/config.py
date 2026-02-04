import getpass

# ORA2PG Data Mapping
data_mapping = {
    "NUMBER" : "numeric",
    "CHAR" : "char",
    "NCHAR" : "char",
    "VARCHAR" : "text",  ## Hier könnte es probleme geben
    "NVARCHAR" : "varchar",
    "VARCHAR2" : "text",  ## Unterschiede Varchar2 und Varchar bei oracle - 
    "NVARCHAR" : "varchar",
    "NVARCHAR2" : "varchar",
    "STRING" : "varchar",
    "DATE" : "timestamp",
    "LONG" : "text",
    "LONG RAW" : "bytes",
    "CLOB" : "text",
    "NCLOB" : "text",
    "BLOB" : "bytea",
    "BFILE" : "bytea",
    "RAW(16)" : "uuid",
    "RAW(32)" : "uuid", 
    "RAW" : "bytea",
    "ROWID" : "oid",
    "UROWID" : "oid", 
    "FLOAT" : "double precision",
    "DEC" : "decimal",
    "DECIMAL" : "decimal",
    "DOUBLE PRECISION" : "double precision", 
    "INT" : "integer",
    "INTEGER" : "integer", 
    "BINARY_INTEGER" : "integer",
    "PLS_INTEGER" : "integer", 
    "SMALLINT" : "smallint",
    "REAL" : "real", 
    "BINARY_FLOAT" : "numeric", 
    "BINARY_DOUBLE" : "numeric", 
    "TIMESTAMP" : "timestamp", 
    "BOOLEAN" : "boolean",
    "INTERVAL" : "interval", 
    "XMLTYPE" : "xml", 
    "TIMESTAMP WITH TIME ZONE" : "timestamp with time zone", 
    "TIMESTAMP WITH LOCAL TIME ZONE" : "timestamp with time zone",
    "SDO_GEOMETRY" : "geometry",
    "ST_GEOMETRY" : "geometry"
}


oracle_connection_data = {
    "un" : "ADMIN_ALL",
    "cs" : "localhost/xepdb1",
    "pw" : ""
}

postgres_connection_data = {
    "database_name" : "postgres",
    "user" : "postgres",
    "password" : "",
    "host" : "localhost",
    "port" : "5432"
}

