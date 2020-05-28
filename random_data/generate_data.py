# -*- coding: utf-8 -*-
"""
Created on Sun May 24 18:43:24 2020

@author: ryhow
"""

import pandas as pd
import datetime
import random 
import os
from numpy.random import choice
from os.path import isfile, join

        
def generate_daily_invoices(intervals, start_date,store_id):
    ### Generates a list of transcation times from a randomly generated 'intervals' parameter
    end_date = start_date + ' 23:59:59'
    daily_times = list(pd.date_range(start=start_date, end = end_date ,periods=intervals))
       
    invoices = []
    ### For each transaction build an invoice statement
    for purchase_start in daily_times: 
        
        ### This randomly generates a purchase end date
        purchase_end = purchase_start + random.randint(1,10) * datetime.timedelta(minutes=1)
        start_ = purchase_start.strftime('%Y-%m-%d %H:%M:%S')
        end_ = purchase_end.strftime('%Y-%m-%d %H:%M:%S')
        invoice = [start_, end_ ,store_id]
    
        invoices.append(invoice)
        
    ### Make the dataframe
    df = pd.DataFrame(invoices, columns = ['purchase_start', 'purchase_end', 'store_id'])
        
    return df
        

def generate_yearly_data(store_id):
    

    ### These dates are arbitrary begin and end dates for the date range
    start_dt = datetime.date(2019, 1, 1)
    end_dt = datetime.date(2019, 12, 31)
    intervals = 365
    daily_times = list(pd.date_range(start=start_dt, end = end_dt ,periods=intervals))
    ### Generate the date range
    dates = [dt.strftime("%Y-%m-%d") for dt in daily_times]
    ### Randomly generate the # of transcactions for a given store in a day
    invoice_nums = [random.randint(100,200) for i in range(len(dates))]
    
    invoice_data = []
    t1 = datetime.datetime.now()
    print('Time now -', t1)
    print('Store # -', store_id)
    print('\n')
    ### Iterate through and generate one stores yearly data
    for (inv_num, date) in list(zip(invoice_nums, dates)):
       
    
        ### Get all invoices day
        invoices = generate_daily_invoices(inv_num, date, store_id)    
        invoice_data.append(invoices)
        
    end_result = pd.concat(invoice_data)
    return end_result

def generate_purchase(invoices):
    ### Generate a list of purchase for a given invoice (generates random product ids)

    n = 100000
    
    purchases = [random.choice(invoices) for i in range(n)]
    weights = [.19]+ [.08]*10 + [.01] 
    product_choices = list(range(1,13))
    
    products = choice(product_choices, n,
              p=weights)

   
    
    data = {'product_id': products, 'invoice_id' : purchases}
    df = pd.DataFrame(data)
    return df

def main():
    

    ### This is arbitrary (originally intended it to be much larger but it's time consuming)
    stores = list(range(1,51))
    invoice_data = list(map(generate_yearly_data, stores))
    
    ### Makes invoices
    invoice_result = pd.concat(invoice_data)
    #n = 2500000
    num_rows = range(len(invoice_result))
    invoice_result.to_csv("invoice_sample.csv", index=False)
    
    chunk_size = 50000
    purchase_chunks = [num_rows[i:i + chunk_size] for i in range(0, len(num_rows), chunk_size)]
    i = 1
    path = os.getcwd()
    
    for chunk in purchase_chunks:
        data = generate_purchase(chunk)
        filename = 'purchase_data\\purchase_{num}.csv'.format(num=i)

        data_name = os.path.join(path, filename)
        data.to_csv(data_name, index=False)
        i+=1
    
    ### Makes the store data
    locations = [random.randint(1,51) for i in stores]
    data  = {'id': stores, 'store_id':locations}
    store_result = pd.DataFrame(data)

    store_result.to_csv("store_locations.csv", index=False)
if __name__ == "__main__":
    main()



    
