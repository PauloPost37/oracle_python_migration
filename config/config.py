#This file contains mapping rules derived from Ora2Pg (https://ora2pg.darold.net/),
#which is licensed under the GNU General Public License v3.0.

#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, version 3.

#A copy of the GPLv3 license should be included in the LICENSE file.

data_mapping = {
    "NUMBER" : "numeric",
    "CHAR" : "char",
    "NCHAR" : "char",
    "VARCHAR" : "varchar",  
    "NVARCHAR" : "varchar",
    "VARCHAR2" : "varchar",  
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

# Expanded by Gemini 3 Pro
oracle_connection_data = {
    "un" : "ADMIN_ALL",
    "cs" : "localhost/xepdb1",
    "pw" : "abcd1234",
    "host" : "localhost",
    "port" : "1521",
    "sid" : "xe",
    "use_sid" : False
}


postgres_connection_data = {
    "database_name" : "postgres",
    "user" : "postgres",
    "password" : "abcd1234",
    "host" : "localhost",
    "port" : "5432"
} 

