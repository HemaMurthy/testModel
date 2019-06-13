# -*- coding: utf-8 -*-
#!/usr/bin/env python

import os
import sys
import pandas as panda
import numpy as np
import feature_helper
import report
import datetime as dt
from datetime import datetime,timedelta
import random

#fetching data from csv files
products,notes,product_status,asset_features,condition_events,firmware_history,life_events,service_record = feature_helper.read_from_csv_files()

report.create_report()
report.write_into_report('\nTotal assets: ',len(products))
dates=[]
for x in notes['date']:
        dates.append(datetime.strptime(x,'%Y-%m-%d'))
dates.pop(0)
min_date=min(dates)
max_date=max(dates)
now = dt.datetime.now()
report.write_into_report('\nReport date: ',now)
report.write_into_report('\n\nDaily Work gathered from: ',min_date.date())
report.write_into_report('\nLast Daily work collected on:',now.date())
         
#reason:1 active assets
active_assets = products.customer_asset_identifier[(products.active_status == 'Active')]
report.write_into_report('\n\nActive assets: ',len(active_assets))

error_log=products.customer_asset_identifier[(products.active_status != 'Active')]
error_log=panda.DataFrame(error_log)
report.write_into_report('\nInactive assets: ',len(error_log))

#storing error'ed assets into error_log.csv
error_log.insert(1,'reason_id',1,True)

#reason:2 log status manual alone
daily_prod=panda.DataFrame(life_events[(life_events.life_event_code==3) & (life_events.life_event_value==1)])
man_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code==3) & (life_events.life_event_value==2)])
log2_list=list(set(man_prod['customer_asset_identifier'])-set(daily_prod['customer_asset_identifier']))
log1_list=list(set(daily_prod['customer_asset_identifier'])-set(man_prod['customer_asset_identifier']))
manual_df=panda.DataFrame(log2_list)
manual_df.columns=['customer_asset_identifier']
manual_df.insert(1,'reason_id',2,True)
error_log=error_log.append(manual_df)

report.write_into_report('\n\nAssets with only log status 1, ie, only daily entries: ',len(log1_list))
report.write_into_report('\nAssets with only log status 2, ie, only Manual entries: ',len(log2_list))

#reason:3 log status daily but no notes data for n=15 days
no_notes=[]
perfect_notes=[]
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
                no_notes.append(product)
                print 'traps =',trap
        else:
                perfect_notes.append(product)
report.write_into_report('\n\nAssets with missing work entries for more than 15 days: ',len(no_notes))
report.write_into_report('\nAssets with Perfect work entries: ',len(perfect_notes))
report.write_me('\n[*Perfect work meaning no gap in daily work entries for more than 15 days]')
no_notes=panda.DataFrame(no_notes)
no_notes.columns=['customer_asset_identifier']
no_notes.insert(1,'reason_id',3,True)
error_log=error_log.append(no_notes)

#reason: 4 no notes summary for atleast 60 days
no_notes_list=[]
notes_60=[]
avail_notes=notes['customer_asset_identifier'].drop_duplicates()
for product in avail_notes:
          asset_notes=notes[(notes.customer_asset_identifier==product)]
          if( len(asset_notes)>60):
            notes_60.append(product)
          else:
            no_notes_list.append(product)
report.write_into_report('\n\nAssets with work entries for atleast 60 days: ',len(notes_60))
report.write_into_report('\nAssets without work entries for atleast 60 days: ',len(no_notes_list))
no_notes_list=panda.DataFrame(no_notes_list)
no_notes_list.columns=['customer_asset_identifier']
no_notes_list.insert(1,'reason_id',4,True)
error_log=error_log.append(no_notes_list)


#reason: 5 insatll date in the future
install_date_error=products.customer_asset_identifier[products.installed_date>now.date()]
report.write_into_report('\n\nAssets with install dates in the future: ',len(install_date_error))
install_date_error=panda.DataFrame(install_date_error)
install_date_error.columns=['customer_asset_identifier']
install_date_error.insert(1,'reason_id',5,True)
error_log=error_log.append(install_date_error)
print '..Completed processing all filters..'

#validate using a random number
print '..Validating with a random number..'
random_prod=random.choice(products.customer_asset_identifier)
print 'using random customer_asset_identifier number: ',random_prod
report.write_into_report('\n\nRandom Product id: ',random_prod)
at_focus=error_log.loc[error_log['customer_asset_identifier']==random_prod]
if at_focus.empty:
        report.write_me('\nProduct chosen for feature engineering!')
else:
        report.write_me('\nProduct found in error log!\nError log: \n')
        report.write_me(error_log.loc[error_log['customer_asset_identifier']==random_prod])
report.write_me('\nRandom Product Information:')
report.write_me(products.loc[products.customer_asset_identifier==random_prod])
report.write_me('\n\nRandom Product Life Events:')
report.write_me(life_events.loc[life_events.customer_asset_identifier==random_prod])

#write error_log into .csv file
print 'creating error_log csv file'
prods=products.customer_asset_identifier
df=panda.DataFrame(error_log)
df.to_csv('error_log.csv',index=False)

#get valuable products
error_log_list=error_log['customer_asset_identifier']
#error_log_list=error_log_list[error_log_list.reason_id!=3]

error_log_list=list(set(prods)&set(error_log_list))
report.write_into_report('\n\nAssets in error_log: ',len(error_log_list))
print 'assessing valid products'
valid=list(set(prods)-set(error_log_list))
valid=panda.DataFrame(valid).drop_duplicates()
report.write_into_report('\nAssets passing all filters: ',len(valid))

valid.to_csv('valid_assets.csv',index=False)

report.write_me('\n\nReason Code\tDescription\n1\tNot active now\n2\tLog status manual\n3\tLog status Daily but no data for the last 15 days\n4\tDon’t have last 60 days  of daily note\n5\tInstall date in the future\n\n\t\t\t---END OF REPORT---')

print 'All done!'
