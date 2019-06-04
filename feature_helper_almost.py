
# -*- coding: utf-8 -*-
#!/usr/bin/env python

import pandas as panda
import numpy as np
import sys
import os
from datetime import datetime, timedelta
import time
import asset_report

def true_log_filter(life_events):
        #to check for un-interrupted log status 2 for more than 15 days
        life_events.sort_values(by='customer_asset_identifier')
        product_id=life_events.customer_asset_identifier
        value=0
       log_list=[]
        event_code=life_events.life_event_code
        for x in event_code:
                if x==3:
                   log_list.append(product_id[value])
                value+=1
        #return lists of all life_event_code 3 for log status
        #TODO: check if 15 day gap exists  
        return log_list
       
def daily_notes_filter(products,notes):
        print("Filter 3 \tChecking for assets without daily notes summary.....")
        notes_list=notes['customer_asset_identifier']
        products_list=products['customer_asset_identifier']
        error_log=list(set(notes_list)-set(products_list))
        asset_report.write_into_report("\n\nAssets without daily notes summary: ",len(error_log))
        new_prods= set(notes_list)&set(products_list)
        return new_prods

def install_date_filter(products):
        print("Filter 2 \tChecking for assets installed after 2010-01-01.....")
        products_ids=products.customer_asset_identifier
        dates=products.installed_date
        value=0
        error_log=[]
        new_prods=[]
        LimitDate=datetime(2010,01,01).date()
        for x in dates:
            if x> LimitDate:
                new_prods.append(products_ids[value])
            else:
                error_log.append(products_ids[value])
            value+=1
        asset_report.write_into_report("\n\nAssets installed after 2010-01-01: ",len(new_prods))
        asset_report.write_into_report("\nAssets installed before 2010-01-01: ",len(error_log))
        #return asset_report.make_dataFrame(products,new_prods)
        return new_prods

def active_filter(products):
        print("Filter 1 \tEvaluating active assets.....")
        product_ids=products.customer_asset_identifier
        status=products.active_status
        value=0
        error_log=[]
        active_products=[]
        asset_report.write_into_report("\nTotal assets: ",len(products))
        for x in status:
            if x=='Active':
                active_products.append(product_ids[value])
            else:
                error_log.append(product_ids[value])
            value+=1
        #error_log contains product_ids of inactive assets 
        asset_report.write_into_report("\nActive assets: ",len(product_ids)-len(error_log))
        asset_report.write_into_report("\nInactive assets: ",len(error_log))
        return active_products
        #send error log to make csv

def log_status_filter(life_events):
        print("Filter 4 \tChecking for log status 1.....")
        products_ids=life_events.customer_asset_identifier
        life_code=life_events.life_event_code
        life_value=life_events.life_event_value
        value=0
        new_prods=[]
        error_log=[]
        for x in products_ids:
            if life_code[value]==3:
                if life_value[value]==2:
                        error_log.append(products_ids[value])
                else:
                        new_prods.append(products_ids[value])
            value+=1
        asset_report.write_into_report("\n\nAssets with log status 1 (daily): ",len(new_prods))
        asset_report.write_into_report("\nAssets with log status 2 (manual): ",len(error_log))
        #send error_log to make csv
        return new_prods

def filter(products,notes,life_events):
   asset_report.create_report()
   try:
        active_products=active_filter(products)
        installed_products=install_date_filter(products)
        notes_product=daily_notes_filter(products,notes)
        log_1_products=log_status_filter(life_events)
        true_log_products=true_log_filter(life_events)
        #log status=1 for more than 15 consecutive dates
        #install date in the future
        product_list=list(set(active_products)&set(installed_products)&set(notes_product)&set(log_1_products))
        asset_report.write_into_report("\n\nValid assets: ",len(product_list))
        return products_list
   except Exception as e:
        raise

def read_from_csv_files():
   try:
        data_dir='/home/hema_murthy/testModel/testDataSource'
        notes=panda.read_csv(os.path.join(data_dir,'asset_daily_work.csv'))
        products=panda.read_csv(os.path.join(data_dir,'asset_ib_info.csv'))
        products.installed_date=panda.to_datetime(products.installed_date).dt.date
        product_status=products['active_status']
        product_status.append(products['customer_asset_identifier'])
        life_events=panda.read_csv(os.path.join(data_dir,'asset_life_event.csv'))
        asset_features= panda.read_csv(os.path.join(data_dir,'asset_features.csv'))
        errors=panda.read_csv(os.path.join(data_dir,'asset_conditions_events.csv')) #contains error code info
        firmware_history=panda.read_csv(os.path.join(data_dir,'firmware_history.csv'))
        firmware_history = firmware_history[(~firmware_history['created_date'].isnull())]
        firmware_history = firmware_history[(~firmware_history['created_date'].str.contains('0000-00-00 00:00:00'))]
        service_record=panda.read_csv(os.path.join(data_dir,'service_request_record.csv'))
        return products,notes,product_status,asset_features,errors,firmware_history,life_events,service_record
   except Exception as e:
        print 'Error in reading data from csv file', e.message
        raise
