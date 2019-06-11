

print log1.head(4)

import os
import sys
import pandas as panda
import numpy as np
import feature_helper
import asset_report
import datetime as dt
import random

#fetching data from csv files
products,notes,product_status,asset_features,condition_events,firmware_history,life_events,service_record = feature_helper.read_from_csv_files()

asset_report.create_report()
asset_report.write_into_report('Total assets: ',len(products))

#reason:1 active assets
active_assets = products.customer_asset_identifier[(products.active_status == 'Active')]
asset_report.write_into_report('Active assets: ',len(active_assets))

error_log=panda.DataFrame(products.customer_asset_identifier[(products.active_status != 'Active')])
asset_report.write_into_report('Total assets: ',len(products))

#write into report.txt

#storing error'ed assets into error_log.csv
error_log.insert(1,'reason_id',1,True)
  
#reason:2 log status manual alone
daily_prod=panda.DataFrame(life_events[(life_events.life_event_code==3) & (life_events.life_event_value==1)])
man_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code==3) & (life_events.life_event_value==2)])
log2_list=list(set(man_prod['customer_asset_identifier'])-set(daily_prod['customer_asset_identifier']))

man_prod.insert(1,'reason_id',2,True)
error_log=error_log.append(man_prod)

log1_list=list(set(daily_prod['customer_asset_identifier'])-set(man_prod['customer_asset_identifier']))

#log status 1 through out it's lifetime
daily_prod=prod.customer_asset_identifier[(prod.life_event_value==1)]
log1=prod[prod.customer_asset_identifier==daily_prod

#reason:3 log status daily but no notes data for n=15 days
no_notes=[]          
for product in daily_prod:
          install_date=products.installed_date[(products.customer_asset_identifier==product)]
          product_work=notes.working_unit[(products.customer_asset_identifier==product)]
          flag=0
          for data in product_work:
            if data.isnan():
                flag+=1
            if flag<15:
                no_notes.append(product)
                
#no notes summary for atleast 60 days
no_notes_list=[]      
notes_60=[]   
for product in products.customer_asset_identifier:
          asset_notes=notes[(notes.customer_asset_identifier==product)]
          if( len(asset_notes)>60):
            notes_60.append(product)
          else:
            no_notes_list.append(product)
#insatll date in the future
  
now = dt.datetime.now()
install_date_error=products.customer_asset_identifier[(products.installed_date>now)]
#print report / write into report.txt 
newData=panda.DataFrame(products, columns=['active_status','installed_date'])
active_count=0
print 'Total Assets: ',len(newData)
for x in newData.active_status:
  if x == 'Active':
       active_count +=1
print 'Active assets: ',active_count
count =0
print 'Assets with install date after 2010-01-01 :'
LimitDate=dt.date(2010,01,01)
for x in newData.installed_date:
  if x > LimitDate:
        count +=1
print count
#filters

#validate using a random number
random_prod=random.choice(prod_list)
print products[products.customer_asset_identifier==random_prod]
print condition_events[condition_events.customer_asset_identifier==random_prod] 
print life_events[life_events.customer_asset_identifier==random_prod]                                  
