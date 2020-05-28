# -*- coding: utf-8 -*-
"""
Created on Wed May 27 19:00:02 2020

@author: ryhow
"""

import pandas as pd
import mysql.connector
import os
import random

def insert_purchases(batch_size, cursor, cnx):
    
    path =os.getcwd()
    path = os.path.dirname(path) 
    mypath= join(path, 'purchase_data')
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    i = 1
    for file in onlyfiles:
        filepath =join(mypath, file)
        df = pd.read_csv(filepath)
        print('\n')
        ### Insertion
        print('Insert #', i)
        list_df = [df[i:i+batch_size] for i in range(0,df.shape[0],batch_size)]
        for item in list_df:
            records = list(item.itertuples(index=False, name=None))
                
            q = """ INSERT IGNORE into purchases ( product_id , invoice_id) 
                    values (%s,%s) """
            cursor.executemany(q, records)
            cnx.commit()
        print('\n')
        i+=1
    
        
def insert_invoices(batch_size, cursor, cnx):
    filename = 'invoice_sample.csv'
    cols = ["purchase_start","purchase_end", "store_id"]
    for chunk in pd.read_csv(filename,usecols = cols,chunksize=50000):
        list_df = [chunk[i:i+batch_size] for i in range(0,chunk.shape[0],batch_size)]
        for item in list_df:
        
            records = list(item.itertuples(index=False, name=None))

            q = """ INSERT IGNORE into invoices ( purchase_start,purchase_end,store_id)
                    VALUES (%s,%s,%s) ;"""
            cursor.executemany(q, records)
            cnx.commit()
def insert_stores(cursor, cnx):
    
    def make_stores():
        stores = list(range(1,51))
        location_ids = [random.randint(1,51) for i in stores]
        
        data = {'id': stores, 'location_id': location_ids}
        df = pd.DataFrame(data)
        
        
        return df
    df = make_stores()
    records = list(df.itertuples(index=False, name=None))
    q = """ INSERT IGNORE into stores (id, location_id)
                VALUES (%s, %s)"""
    cursor.executemany(q,records)
    cnx.commit()


def main():
    
    #### MySQL connection
    creds = os.environ['MYSQL_TEST'].split('@#$')
    cnx = mysql.connector.connect(host=creds[0],user=creds[1],password=creds[2],database=creds[3])
    cursor = cnx.cursor()
    
    batch_size =2500
    insert_stores(cursor,cnx)
    insert_invoices( batch_size, cursor, cnx)
    insert_purchases(batch_size, cursor, cnx)
    
    
if __name__ == "__main__":
    main()