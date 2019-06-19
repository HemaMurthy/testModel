import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float
from sqlalchemy.sql import exists
import json

import numpy as np
import pandas as pd
from datetime import date
from datetime import datetime, timedelta
from sklearn.externals import joblib
#from joblib import Parallel, delayed
import argparse
import urllib2
import os
import shutil

    
#changed mldb_creds.json with 
def getSQlEngine():
    with open("mldb_creds.json") as f:
        dbCreds = json.load(f)

    PASSWORD = dbCreds['password']
    DB_NAME = dbCreds['db_name_ml']
    HOST = dbCreds['host']
    USER = dbCreds['user']
    engine = create_engine("mysql+mysqldb://" + USER + ":"+PASSWORD+"@" + HOST + "/"+DB_NAME)
    meta = MetaData(bind=engine)
    return engine, meta
   
DATADIR = '/home/hema_murthy/testModel/testDataSource' #change to the necessary directory 
if not os.path.isdir(DATADIR):
    os.makedirs(DATADIR)
else:
    #delete the older zip files
    zipFiles=os.listdir('./')
    for item in zipFiles:
        if item.endswith(".zip") and item.startswith("DataSource"):
            os.remove(os.path.join('./', item))
    #need to zip all the file and upload it to google drive
    zipdir(DATADIR)
    shutil.rmtree(DATADIR)
    os.makedirs(DATADIR)

engine = create_engine("mysql+mysqldb://" + USER + ":"+PASSWORD+"@" + HOST + "/"+DB_NAME)
meta = MetaData(bind=engine)

def getTableSchema(tableName, meta):
    if tableName == service_request_history: #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('customer_asset_identifier', int(11), nullable=False),
                Column('service_request_date', date, nullable=False),
                Column('incident_category', int(11), nullable=False),
                Column('incident_status', String(10), nullable=False),
                Column('ekryp_cusomter_id', int(11), nullable=False),
                Column('Asset_serial_number', int(11), nullable=False),
                Column('Priority', String(10), nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)
    

dummy=panda.read_csv(os.path.join(DATADIR,'service_request_record.csv')

df = panda.DataFrame({
    'customer_asset_identifier': dummy['customer_asset_identifier'],
    'service_request_date': dummy['service_request_date'],
    'incident_category': dummy['incident_category'],
    'incident_status': dummy['status'],
    'ekryp_cusomter_id':1,
    'Asset_serial_number': dummy['serial_number'],
    'Priority': dummy['priority']
})

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema('TABLENAME',df,'service_request_history',sqlEngine,sqlMeta)
pushToSQL(tableSchema,df,'service_request_history',sqlEngine,sqlMeta)

print 'all done'

'''
#For the list of product Ids we would need to make a call to the API for the last day of the training range to
#get the list of products for which the model needs to be trained

#list of tables which needs to be pulled 'service_request_record', 'firmware_history', 'asset_conditions_events', 'asset_daily_work',
#            'asset_features',
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
    
'''
