
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

error_log=panda.DataFrame(products.customer_asset_identifier[(products.active_status != 'Active')])
report.write_into_report('\nInactive assets: ',len(error_log))

error_log.columns=['customer_asset_identifier'] 
#storing error'ed assets into error_log.csv
error_log.insert(1,'reason_id',1,True)

#reason:2 log status manual alone
daily_prod=panda.DataFrame(life_events[(life_events.life_event_code==3) & (life_events.life_event_value==1)])
man_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code==3) & (life_events.life_event_value==2)])
log2_list=list(set(man_prod['customer_asset_identifier'])-set(daily_prod['customer_asset_identifier']))

man_prod.insert(1,'reason_id',2,True)
error_log=error_log.append(man_prod)

log1_list=list(set(daily_prod['customer_asset_identifier'])-set(man_prod['customer_asset_identifier']))
report.write_into_report('\n\nAssets with only log status 1, ie, only daily entries: ',len(log1_list))
report.write_into_report('\nAssets with only log status 2, ie, only Manual entries: ',len(log2_list))

#reason:3 log status daily but no notes data for n=15 days
def consecutive(a,b,step=dt.timedelta(days=15)):
        return (a+step)==b

no_notes=[]
perfect_notes=[]
for product in products.customer_asset_identifier:
          install_date=products.installed_date[(products.customer_asset_identifier==product)]
          product_work=panda.DataFrame(notes[(notes.customer_asset_identifier==product)])
          flag=0
          dates=[]
          for x in product_work['date']:
                  dates.append(datetime.strptime(x,'%Y-%m-%d'))
          if all(consecutive(dates[i], dates[i+1]) for i in xrange(len(dates) - 1)):
                        perfect_notes.append(product)
          else:
                        no_notes.append(product)
report.write_into_report('\n\nAssets with missing work entries for more than 15 days: ',len(no_notes))
no_notes=panda.DataFrame(no_notes)
no_notes.insert(1,'reason_id',3,True)
error_log=error_log.append(no_notes)

#reason: 4 no notes summary for atleast 60 days
no_notes_list=[]
notes_60=[]
for product in products.customer_asset_identifier:
          asset_notes=notes[(notes.customer_asset_identifier==product)]
          if( len(asset_notes)>60):
            notes_60.append(product)
          else:
            no_notes_list.append(product)
report.write_into_report('\n\nAssets with work entries for atleast 60 days: ',len(notes_60))
report.write_into_report('\nAssets without work entries for atleast 60 days: ',len(no_notes_list))
no_notes_list=panda.DataFrame(no_notes_list)
no_notes_list.insert(1,'reason_id',4,True)
error_log=error_log.append(no_notes_list)


#reason: 5 insatll date in the future
install_date_error=products.customer_asset_identifier[products.installed_date>now.date()]
report.write_into_report('\n\nAssets with install dates in the future: ',len(install_date_error))
install_date_error=panda.DataFrame(install_date_error)
install_date_error.insert(1,'reason_id',5,True)
error_log=error_log.append(install_date_error)

#validate using a random number
random_prod=random.choice(products.customer_asset_identifier)
report.write_into_report('\n\nRandom Product id: ',random_prod)
at_focus=error_log.loc[error_log['customer_asset_identifier']==random_prod]
if at_focus.empty()==True:
        report.write_me('\nProduct chosen for feature engineering!')
else:
        report.write_me('\nProduct found in error log!\nError log: \n')
        report.write_me(error_log.loc[error_log['customer_asset_identifier']==random_prod])
report.write_me('\nRandom Product Information:')
report.write_me(products.loc[products.customer_asset_identifier==random_prod])
report.write_me('\n\nRandom Product Life Events:')
report.write_me(life_events.loc[life_events.customer_asset_identifier==random_prod])

df=panda.DataFrame(error_log)
df.to_csv('error_log.csv',index=True, index_label=['customer_asset_identifier','Reason Id'])

