
import pandas as panda
import sys
import os
import datetime as dt
import asset_report

def daily_notes_filter(products,notes):
        print("Filter 4 \tChecking for assets without daily notes summary.....")
        notes= notes.drop(columns=["id","ekryp_partner_id","created_at","updated_at"])
        newData=notes.xs(product,drop_level=False).working_unit
        print len(newData)
        #error_log=[]
        #asset_report.write_into_report("\nAssets without daily notes summary: ",len(error_log))
    
'''
def install_date_filter(products):
        print("Filter 3 \tChecking for assets installed after 2010-01-01.....")
        products_ids=products.customer_asset_identifier
        dates=products.installed_date
        value=0
        error_log=[]
        new_prods=[]
        LimitDate=dt.date(2010,01,01)
        for x in dates:
            if x> LimitDate:
                new_prods.append(products_ids[value])
            else:
                error_log.append(products_ids[value])
            value+=1
        asset_report.write_into_report("\n\nAssets installed after 2010-01-01: ",len(new_prods))
        asset_report.write_into_report("\nAssets installed before 2010-01-01: ",len(error_log))
            
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

def log_status_filter(life_events):
        print("Filter 2 \tChecking for log status 1.....")
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
        return new_prods
'''
def filter(products,notes,life_events):
   asset_report.create_report()
   try:
        ##active_products=active_filter(products)
        ##log_1=log_status_filter(life_events)
        ##installed_after_2010=install_date_filter(products)
        notes_present=daily_notes_filter(products,notes)
        #with no daily notes summary
        #log status=1 for more than 15 consecutive dates
        #install date in the future
        return products,notes,life_events
   except Exception as e:
        raise
       
def read_from_csv_files():
   try:
        data_dir='/home/hema_murthy/testModel/testDataSource'
        notes=panda.read_csv(os.path.join(data_dir,'asset_daily_work.csv'))
        #notes['date'] = panda.to_datetime(notes['date']).dt.date
        #notes.set_index(['customer_asset_identifier','date'],drop=True,inplace=True)
        #notes contains col1:customer_asset_id and col2: date

        products=panda.read_csv(os.path.join(data_dir,'asset_ib_info.csv'))
        products.installed_date=panda.to_datetime(products.installed_date).dt.date
        product_status=products['active_status']
        product_status.append(products['customer_asset_identifier'])
        life_events=panda.read_csv(os.path.join(data_dir,'asset_life_event.csv'))
        asset_features= panda.read_csv(os.path.join(data_dir,'asset_features.csv'))
        condition_events=panda.read_csv(os.path.join(data_dir,'asset_conditions_events.csv')) #contains error code info
        firmware_history=panda.read_csv(os.path.join(data_dir,'firmware_history.csv'))
        return products,notes,product_status,asset_features,condition_events,firmware_history,life_events
   except Exception as e:
        print 'Error in reading data from csv file', e.message
        raise
