#done with service-request-history

import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import json

import numpy as np
import pandas as panda
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
    
#directory where ekryp-data-db-prod tables are stored
DATADIR = '/home/hema_murthy/testModel/testDataSource' #change to the necessary directory 

#alter this to match the table structure
def getTableSchema(tableName, meta):
    if tableName == 'service_request_history': #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('customer_asset_identifier',Integer, nullable=False),
                Column('service_request_date',String(10), nullable=False),
                Column('incident_category', String(20), nullable=False),
                Column('incident_status', String(10), nullable=False),
                Column('ekryp_customer_id', Integer, nullable=False),
                Column('Asset_serial_number', String(15), nullable=False),
                Column('Priority', String(10), nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)

#load table info from ekryp_data_db_prod!!! i've already done that, and i have it on my GCP directory
dummy=panda.read_csv(os.path.join(DATADIR,'service_request_record.csv'))

#alter this to fit the info in df to match the columns in table
df = panda.DataFrame({
    'customer_asset_identifier': dummy['customer_asset_identifier'],
    'service_request_date': dummy['service_request_date'],
    'incident_category':dummy['incident_category'],
    'incident_status': dummy['status'],
    'ekryp_customer_id':1,
    'Asset_serial_number':dummy['serial_number'],
    'Priority': dummy['priority']
})

#table you want to populate
tablename='service_request_history'

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema(tablename,sqlMeta)
pushToSQL(tableSchema,df,'service_request_history',sqlEngine,sqlMeta)

print 'All done'
