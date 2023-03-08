import pymysql
import os
from dotenv import load_dotenv

def get_env_variables():

    load_dotenv()
    HOST = os.environ.get("mysql_host")
    USER = os.environ.get("mysql_user")
    PASSWORD = os.environ.get("mysql_pass")
    WAREHOUSE_DB_NAME = os.environ.get("mysql_db")

    return HOST, USER, PASSWORD, WAREHOUSE_DB_NAME

def connect():
    inHOST, inUSER, inPASSWORD, inWAREHOUSE_DB_NAME = get_env_variables()
    
    connection = pymysql.connect(
        host=inHOST,
        user=inUSER,
        password=inPASSWORD,
        db=inWAREHOUSE_DB_NAME)

    cursor = connection.cursor()
    return connection, cursor




