import oracledb

def establish_oracle_connection(un, pw, cs):
    #dsn2 = oracledb.makedsn(host, port, sid)
    try:
        return oracledb.connect(user=un, password=pw, dsn=cs)
    except oracledb.DatabaseError as e:
        raise