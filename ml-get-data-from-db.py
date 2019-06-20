#!/usr/bin/env python
'''
This script gets data from the MYSQL databases for feature generation as per the parameters provided
'''
import numpy as np
import pandas as pd
from datetime import date
from datetime import datetime, timedelta
from sklearn.externals import joblib
#from joblib import Parallel, delayed
import argparse
import urllib2
import json

import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float
from sqlalchemy.sql import exists
import os
import shutil

def zipdir(datadir):
    timeStamp =  datetime.fromtimestamp(os.path.getmtime(datadir)).strftime('%Y%m%d_%H_%M_%S')
    nFile = datadir + timeStamp
    shutil.make_archive(nFile, 'zip', datadir)

#Updates the name of the file with latest with the dateit was created
def update_latest_file(metaDataFile, comment, latestFileName, columns):
    data_dir = '../DataSets'
    f = filter(lambda x: 'latest' in x, os.listdir(data_dir))[0]
    fpath = os.path.join(data_dir, f)
    timeStamp =  datetime.fromtimestamp(os.path.getmtime(fpath)).strftime('%Y%m%d_%H_%M_%S')
    meta = pd.read_csv(os.path.join(data_dir, metaDataFile))
    nFile = f.replace('latest', timeStamp)
    meta['fileName'].loc[meta.fileName.str.contains('latest')] = nFile
    meta.loc[meta.shape[0]] = [latestFileName, comment, columns]
    meta.to_csv(os.path.join(data_dir, metaDataFile), index=False)



def get_db_creds(credFile):
    with open(credFile) as f:
        dbCreds = json.load(f)
    return dbCreds['password'], dbCreds['db_datasource'], dbCreds['host'], dbCreds['user']

PASSWORD, DB_NAME, HOST, USER = get_db_creds("db_creds.json")
DATADIR = 'ml-DataSource'
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)
else:
    #delete the older zip files
    zipFiles=os.listdir('./')
    for item in zipFiles:
        if item.endswith(".zip") and item.startswith("ml-DataSource"):
            os.remove(os.path.join('./', item))
    #need to zip all the file and upload it to google drive
    zipdir(DATADIR)
    shutil.rmtree(DATADIR)
    os.makedirs(DATADIR)
#For the list of product Ids we would need to make a call to the API for the last day of the training range to
#get the list of products for which the model needs to be trained

#list of tables which needs to be pulled
tableNames = ['service_request_record', 'firmware_history', 'asset_conditions_events', 'asset_daily_work',
             'asset_features', 'asset_ib_info','service_order_record','field_service_ops_activity_record',
             'site_record','company_record','mapping_code_failure_mapping']

engine = create_engine("mysql+mysqldb://" + USER + ":"+PASSWORD+"@" + HOST + "/"+DB_NAME)
meta = MetaData(bind=engine)

for tbl in tableNames:
    print 'Fetching file', tbl
    #Select = ("SELECT * FROM "+ tbl +" ;")
    tblDf = []
    for chunk in pd.read_sql_table(tbl, engine, chunksize = 10000):
        tblDf.append(chunk)
    tblDf = pd.concat(tblDf)
    outputPath = os.path.join(DATADIR, tbl+'.csv')
    tblDf.to_csv(outputPath, index=False)
    print tbl, 'fetched, # of rows fetched', tblDf.shape[0]
    print '-'*53
