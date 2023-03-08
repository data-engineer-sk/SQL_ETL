import pandas as pd
import connect_to_db as conn_db
import db_actions as db

def db_operations():

    connection, cursor = conn_db.connect()
    
    #------------------------ SELECT BLOCK --------------------------------
    age = 29
    sql_statement = 'select * from person where age = %s'
    val = (age)
    #------------------------ End of SELECT BLOCK -------------------------
    
    #------------------------ INSERT BLOCK --------------------------------
    # first_name = 'David'
    # last_name = 'Chan'
    # age = 18
    # email = 'david.chan@home.com'
    # sql_statement = "INSERT INTO person (first_name, last_name, age, email) VALUES (%s,%s,%s,%s)" 
    # val = (first_name, last_name, age, email)
    #------------------------ End of INSERT BLOCK -------------------------

    return_data = db.select_sql(connection, cursor, sql_statement, val)
    # return_data = db.insert_sql(connection, cursor, sql_statement, val)

def main():
    db_operations()

main()