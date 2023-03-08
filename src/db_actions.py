import pymysql

def select_sql(connection, cursor, sql_statement, val):
    rtn = 0
    try:
        if val == '':
            rtn = cursor.execute(sql_statement)
        else:
            rtn = cursor.execute(sql_statement, val)

        db_data = cursor.fetchall()
        for fetch_rows in db_data:
            print(fetch_rows)

        connection.commit()
    except pymysql.err as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

    return rtn 

def insert_sql(connection, cursor, sql_statement, val):
    rtn = 0
    try:
        rtn = cursor.execute(sql_statement, val)
        cursor.fetchall()

        connection.commit()
    except pymysql.err as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

    return rtn

# Update for future
def update_sql(connection, cursor, sql_statement, val):
    rtn = 0

    return rtn

# Delete for future
def delete_sql(connection, cursor, sql_statement, val):
    rtn = 0

    return rtn