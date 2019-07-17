# -*- coding: utf-8 -*-
#!/usr/bin/env python
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import os
import sys
import pandas as panda
import numpy as np
import dask as dd

import report
import datetime as dt
from datetime import datetime,timedelta
import random

def getFromSQL():
    try:
        engine=create_engine("mysql://root1:Ekryp#1234@35.199.174.191/ml_reference")
        conn=mysql.connector.connect( host='35.199.174.191', database='ml_reference',user='root1',password='Ekryp#1234')

        products=panda.read_sql("select customer_asset_identifier, install_date from  ml_reference.asset_information", con= conn)
        products.install_date=panda.to_datetime(products.install_date).dt.date

        life_events=panda.read_sql("select customer_asset_identifier,date, life_event_code, life_event_value from  ml_reference.asset_life_event", con= conn)

        work=panda.read_sql("select customer_asset_identifier,date,work_units from ml_reference.periodic_work_processed", con= conn)
        print 'Successful read from Database.'
        return work, products, life_events
    except Exception as e:
        print 'Error in reading data from Database', e.message
        raise

#fetching data
work,products,life_events= getFromSQL()
work.date=panda.to_datetime(work.date).dt.date
dates=[]
for x in work['date']:
        dates.append(datetime.strptime(str(x),'%Y-%m-%d'))
dates.pop(0)
min_date=min(dates)
max_date=max(dates)
now = dt.datetime.now()
print '\nReport date: ',now
print '\nDaily Work gathered from: ',min_date.date()
print '\nLast Daily work collected on:',now.date()

#assets that have work entries
avail_work=work['customer_asset_identifier'].drop_duplicates()
print '\nTotal assets with daily work: ',len(avail_work)
         
#reason:1 active assets
product_status=panda.DataFrame(life_events[(life_events.life_event_code=='2') & life_events['customer_asset_identifier'].isin(products.customer_asset_identifier)])
#product_status.insert(1,'customer_asset_identifier',life_events['customer_asset_identifier'],True)
      
active_assets= product_status.customer_asset_identifier[(product_status.life_event_value == 1) & life_events['customer_asset_identifier'].isin(products.customer_asset_identifier)].drop_duplicates()
print '\nActive assets: ',len(active_assets)

inactive_list=product_status.customer_asset_identifier[(product_status.life_event_value != 1)& life_events['customer_asset_identifier'].isin(products.customer_asset_identifier)].drop_duplicates()
error_log=panda.DataFrame(inactive_list)
print '\nInactive assets: ',len(error_log)

#storing error'ed assets into error_log.csv
error_log.insert(1,'reason_id',1,True)

#reason:2 log status manual alone
#life_event_
daily_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code=='3') & (life_events.life_event_value==1)& life_events['customer_asset_identifier'].isin(products.customer_asset_identifier)]).drop_duplicates()
man_prod=panda.DataFrame(life_events.customer_asset_identifier[(life_events.life_event_code=='3') & (life_events.life_event_value==2)& life_events['customer_asset_identifier'].isin(products.customer_asset_identifier)]).drop_duplicates()
log2_list=list(set(man_prod['customer_asset_identifier'])-set(daily_prod['customer_asset_identifier']))
daily_alone_list=list(set(daily_prod['customer_asset_identifier'])-set(man_prod['customer_asset_identifier']))
daily_list=list(set(daily_prod['customer_asset_identifier'])&set(man_prod['customer_asset_identifier']))
manual_df=panda.DataFrame(log2_list)
manual_df.columns=['customer_asset_identifier']
manual_df.insert(1,'reason_id',2,True)
error_log=error_log.append(manual_df)

print '\nAssets with only log status 1, ie, only daily entries: ',len(daily_alone_list)
print '\nAssets with only log status 2, ie, only Manual entries: ',len(log2_list)
print '\nAssets with log status 1: ',len(daily_prod)

#reason:3 log status daily but no work data for n=15 days
perfect_work=[]
no_work_list=[]
for product in products.customer_asset_identifier:
        product_work=panda.DataFrame(work[(work.customer_asset_identifier==product)])
        dates=[]
        for x in product_work['date']:
                  dates.append(datetime.strptime(str(x),'%Y-%m-%d'))
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
                no_work_list.append(product)
        else:
                perfect_work.append(product)
print '\nAssets with missing work entries for more than 15 days: ',len(no_work_list)
print '\nAssets with Perfect work entries: ',len(perfect_work)
print '\n[*Perfect work meaning no gap in daily work entries for more than 15 days]'
no_work=panda.DataFrame(no_work_list)
no_work.columns=['customer_asset_identifier']
#no_work.insert(1,'reason_id',3,True)
error_log=error_log.append(no_work)

#reason: 4 no work summary for atleast 60 days
no60_work=[]
work_60=[]
for product in avail_work:
    asset_work=work[(work.customer_asset_identifier==product)]
    if( len(asset_work)>60):
            work_60.append(product)
    else:
            no60_work.append(product)
print '\nAssets with work entries for atleast 60 days: ',len(work_60)
print '\nAssets without work entries for atleast 60 days: ',len(no60_work)
no60_work_list=panda.DataFrame(no60_work)
no60_work_list.columns=['customer_asset_identifier']
no60_work_list.insert(1,'reason_id',4,True)
error_log=error_log.append(no60_work_list)


#reason: 5 insatll date in the future
install_date_error_list=products.customer_asset_identifier[products.install_date>now.date()]
print '\nAssets with install dates in the future: ',len(install_date_error_list)
install_date_error=panda.DataFrame(install_date_error_list)
install_date_error.columns=['customer_asset_identifier']
install_date_error.insert(1,'reason_id',5,True)
error_log=error_log.append(install_date_error)
print '\n..Completed processing all filters..'

#validate using a random number
print '..Validating with a random number..'
random_prod=random.choice(products.customer_asset_identifier)
print 'using random customer_asset_identifier number: ',random_prod
print '\nRandom Product id: ',random_prod
at_focus=error_log.loc[error_log['customer_asset_identifier']==random_prod]
if at_focus.empty:
        print '\nProduct chosen for feature engineering!'
else:
        print '\nProduct found in error log!\nError log: \n'
        print error_log.loc[error_log['customer_asset_identifier']==random_prod]
print '\nRandom Product Information:'
print products.loc[products.customer_asset_identifier==random_prod]
print '\nRandom Product Life Events:'
print life_events.loc[life_events.customer_asset_identifier==random_prod]

#write error_log into .csv file
print 'creating error_log csv file'
prods=products.customer_asset_identifier
df=panda.DataFrame(error_log)
df.to_csv('error_log'+str(datetime.now().date())+'.csv',index=False)


error_log_list=error_log['customer_asset_identifier']

error_log_list=list(set(prods)&set(error_log_list)) #intersection
print '\nAssets in error_log: ',len(error_log_list)
print 'assessing valid products'
valid_list=list(set(prods)-set(error_log_list)) #difference
valid=panda.DataFrame(valid_list).drop_duplicates()
report.write_into_report('\nAssets passing all filters: ',len(valid))

valid.to_csv('valid_assets'+str(datetime.now().date())+'.csv',index=False)  #do we need to include ekryp customer_id here too?

print '\n\nReason Code\tDescription\n1\tNot active now\n2\tLog status manual\n3\tLog status Daily but no data for the last 15 days\n4\tDon’t have last 60 days  of daily note\n5\tInstall date in the future\n\n\t\t\t---END OF REPORT---' #we're fixing these conditions for now
print 'All done!'

def get_data():
        print 'Retriving data..'
        return avail_work,valid_list,error_log_list,list(install_date_error_list),work_60,no60_work,no_work_list,perfect_work,daily_alone_list,daily_list,log2_list
    
def get_valid_assets():
    return valid_list

def get_error_log():
    return error_log_list