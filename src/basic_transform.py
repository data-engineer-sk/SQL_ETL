# Name          : examine_csv.py
# Description   : To examine the given CSV file. e.g. data/MergeCSV.csv
#               : to perform the basic transformation and upload the file
#               : to the MySQL database "SQL-ETL"
# Author        : Samuel Ko
#  
import pandas as pd
import numpy as np
import traceback
import connect_to_db as conn_db
import db_actions as db

def basic_transform():
    try:    
        # 1--------->  Basic Transform ----------
        #   I. Rename the columns
        #   II. Convert the date format
        df = pd.read_csv('../data/MergeCSV.csv')

        # - Step 1) Convert the columns' names
        df.rename(columns = {'Timestamp of Purchase':'TxDate','Store Name':'Store_Name', 'Customer Name':'Cust_Name'}, inplace = True)
        df.rename(columns = {'Basket Items (Name, Size & Price)':'Item','Total Price':'TotPrice', 'Cash/Card':'Payment'}, inplace = True)
        df.rename(columns = {'Card Number (Empty if Cash)':'CardNo'}, inplace = True)

        # - Step 2) Convert integer to string, truncate the last two digit '.0'
        df['CardNo']=df['CardNo'].apply(str).str[:-2]
        # df['CardNo']=df['CardNo'].apply(str)

        # Fill NA with Zero
        # df.fillna(0)

        df.to_csv('../data/MergeCSV_V1.csv', index=False)

        df_v1 = pd.read_csv('../data/MergeCSV_V1.csv')
        

        # Prepare for input elements
        for elements in df_v1.itertuples():
            connection, cursor = conn_db.connect()
            txdate_val = elements.TxDate
            store_name_val = elements.Store_Name
            cust_name_val = elements.Cust_Name
            item_val = elements.Item
            totprice_val = elements.TotPrice
            payment_val = elements.Payment
            cardno_val = elements.CardNo

            # print(f'{txdate_val} {store_name_val} {cust_name_val} {item_val} \
            #        {totprice_val} {payment_val} {cardno_val}')

            inSQL = "INSERT INTO InitialTable (TxDate,Store_Name,Cust_Name,Item,TotPrice,Payment,CardNo) VALUES (%s,%s,%s,%s,%s,%s,%s)" 
            inVal = (txdate_val, store_name_val, cust_name_val, item_val, totprice_val, payment_val, cardno_val)
            result = db.insert_sql(connection, cursor, inSQL, inVal)

    except:
        traceback.print_exc()

if __name__ == "__main__":
    basic_transform()
