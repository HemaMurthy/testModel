
'''
import os
import sys
import pandas as panda
import numpy as np
import feature_helper
import datetime as dt
from feature_helper import dataClass

#fetching data from csv files
products,notes,product_status,asset_features,condition_events,firmware_history = feature_helper.read_from_csv_files()

#print report
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

#store into another csv file
