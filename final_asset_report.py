# -*- coding: utf-8 -*-
#!/usr/bin/env python

import os
import sys
import pandas as panda
import numpy as np
import feature_helper

import datetime as dt
from datetime import datetime,timedelta
import random

#fetching data from csv files
products,notes,product_status,asset_features,condition_events,firmware_history,life_events,service_record = feature_helper.read_from_csv_files()

print('\nTotal assets: ',len(products))
avail_notes=notes['customer_asset_identifier'].drop_duplicates()
print('\nTotal assets with daily work: ',len(avail_notes))
dates=[]
for x in notes['date']:
        dates.append(datetime.strptime(x,'%Y-%m-%d'))
dates.pop(0)
min_date=min(dates)
max_date=max(dates)
now = dt.datetime.now()
print('\nReport date: ',now)
print('\n\nDaily Work gathered from: ',min_date.date())
print('\nLast Daily work collected on:',now.date())
         
#reason:1 active assets
active_assets= products.customer_asset_identifier[(products.active_status == 'Active')]
print('\n\nActive assets: ',len(active_assets))

inactive_list=products.customer_asset_identifier[(products.active_status != 'Active')]
error_log=panda.DataFrame(inactive_list)
print('\nInactive assets: ',len(error_log))

#storing error'ed assets into error_log.csv
error_log.insert(1,'reason_id',1,True)

#reason:2 log status manual alone
daily_prod=panda.DataFrame(life_events[(life_events.life_event_code==3) & (life_events.life_event_value==1)]).drop_duplicates()
man_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code==3) & (life_events.life_event_value==2)]).drop_duplicates()
log2_list=list(set(man_prod['customer_asset_identifier'])-set(daily_prod['customer_asset_identifier']))
daily_alone_list=list(set(daily_prod['customer_asset_identifier'])-set(man_prod['customer_asset_identifier']))
daily_list=list(set(daily_prod['customer_asset_identifier'])&set(man_prod['customer_asset_identifier']))
manual_df=panda.DataFrame(log2_list)
manual_df.columns=['customer_asset_identifier']
manual_df.insert(1,'reason_id',2,True)
error_log=error_log.append(manual_df)

print('\n\nAssets with only log status 1, ie, only daily entries: ',len(daily_alone_list))
print('\nAssets with only log status 2, ie, only Manual entries: ',len(log2_list))
print('\nAssets with log status 1: ',len(daily_prod))

#reason:3 log status daily but no notes data for n=15 days
perfect_notes=[]
no_notes_list=[]
for product in products.customer_asset_identifier:
        product_work=panda.DataFrame(notes[(notes.customer_asset_identifier==product)])
        dates=[]
        for x in product_work['date']:
                  dates.append(datetime.strptime(x,'%Y-%m-%d'))
        flag=0
        trap=0
          #dates=panda.DataFrame(dates
          #print dates[10]
        for it in range(0,len(dates)-1):
                #print 'tomorrow',dates[it+1]
                #print 'today+1 =',dates[it]+timedelta(days=1)
                if(dates[it+1]!=dates[it]+timedelta(days=1)):
                        flag+=1
                        if(flag>15):
                                trap+=1
                                flag=0
                else:
                        flag=0
        if flag>15 or trap>0:
                no_notes_list.append(product)
        else:
                perfect_notes.append(product)
print('\n\nAssets with missing work entries for more than 15 days: ',len(no_notes_list))
print('\nAssets with Perfect work entries: ',len(perfect_notes))
print('\n[*Perfect work meaning no gap in daily work entries for more than 15 days]')
no_notes=panda.DataFrame(no_notes_list)
no_notes.columns=['customer_asset_identifier']
no_notes.insert(1,'reason_id',3,True)
error_log=error_log.append(no_notes)

#reason: 4 no notes summary for atleast 60 days
no60_notes=[]
notes_60=[]
for product in avail_notes:
          asset_notes=notes[(notes.customer_asset_identifier==product)]
          if( len(asset_notes)>60):
            notes_60.append(product)
          else:
            no60_notes.append(product)
print('\n\nAssets with work entries for atleast 60 days: ',len(notes_60))
print('\nAssets without work entries for atleast 60 days: ',len(no60_notes))
no60_notes_list=panda.DataFrame(no60_notes)
no60_notes_list.columns=['customer_asset_identifier']
no60_notes_list.insert(1,'reason_id',4,True)
error_log=error_log.append(no60_notes_list)


#reason: 5 insatll date in the future
products.installed_date = panda.to_datetime(products.installed_date).dt.date
install_date_error_list=products.customer_asset_identifier[products.installed_date>now.date()]
report.write_into_report('\n\nAssets with install dates in the future: ',len(install_date_error_list))
install_date_error=panda.DataFrame(install_date_error_list)
install_date_error.columns=['customer_asset_identifier']
install_date_error.insert(1,'reason_id',5,True)
error_log=error_log.append(install_date_error)
print '..Completed processing all filters..'

#validate using a random number
print '..Validating with a random number..'
random_prod=random.choice(products.customer_asset_identifier)
print 'using random customer_asset_identifier number: ',random_prod
print('\n\nRandom Product id: ',random_prod)
at_focus=error_log.loc[error_log['customer_asset_identifier']==random_prod]
if at_focus.empty:
        print('\nProduct chosen for feature engineering!')
else:
        print('\nProduct found in error log!\nError log: \n')
        print(error_log.loc[error_log['customer_asset_identifier']==random_prod])
print('\nRandom Product Information:')
print(products.loc[products.customer_asset_identifier==random_prod])
print('\n\nRandom Product Life Events:')
print(life_events.loc[life_events.customer_asset_identifier==random_prod])

#write error_log into .csv file
print 'creating error_log csv file'
prods=products.customer_asset_identifier
df=panda.DataFrame(error_log)
df.to_csv('error_log.csv',index=False)

#get valuable products
error_log_list=error_log['customer_asset_identifier']
#error_log_list=error_log_list[error_log_list.reason_id!=3]

error_log_list=list(set(prods)&set(error_log_list)) #intersection
report.write_into_report('\n\nAssets in error_log: ',len(error_log_list))
print 'assessing valid products'
valid_list=list(set(prods)-set(error_log_list)) #difference
valid=panda.DataFrame(valid_list).drop_duplicates()
print('\nAssets passing all filters: ',len(valid))

valid.to_csv('valid_assets.csv',index=False)

print('\n\nReason Code\tDescription\n1\tNot active now\n2\tLog status manual\n3\tLog status Daily but no data for the last 15 days\n4\tDon’t have last 60 days  of daily note\n5\tInstall date in the future\n\n\t\t\t---END OF REPORT---')

print 'All done!'

def get_data():
        print 'Retriving data..'
        return avail_notes,valid_list,error_log_list,list(install_date_error_list),notes_60,no60_notes,no_notes_list,perfect_notes,daily_alone_list,daily_list,log2_list


