import pymssql

def connect(SERVER_NAME,DB_NAME,DB_USER,DB_PASS):
    try:
        conn = pymssql.connect(SERVER_NAME,DB_USER,DB_PASS,DB_NAME)
        return  conn
    except pymssql.OperationalError:
        print("Error in connecting to database")

def executeQuery(conn,query):
    '''conn -> connection object , query-> query to execute. Returns dataset returned from select query'''
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except pymssql.Error as e:
        print("Error Message:Failed to execute query - ", query)

