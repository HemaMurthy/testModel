
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
                Column('ekryp_customer_id', Integer, nullable=False),
                Column('customer_asset_identifier',Integer, nullable=False),
                Column('asset_serial_number', String(15), nullable=False),
                Column('date',String(10), nullable=False),
                Column('code_id', Integer), nullable=False),
                Column('value', Integer), nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)

#load table info from ekryp_data_db_prod!!! i've already done that, and i have it on my GCP directory
events=panda.read_csv(os.path.join(DATADIR,'asset_condition_events.csv'))
count=[]

#alter this to fit the info in df to match the columns in table
df = panda.DataFrame({
    'ekryp_customer_id':1,
    'customer_asset_identifier': events['customer_asset_identifier'],
    'asset_serial_number':events['serial_number'],
    'date': events['date'],
    'code_id': events['code_id'], 
    'value': events['code_count']
})

#table you want to populate
tablename='technician_history'

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema(tablename,sqlMeta)
pushToSQL(tableSchema,df,'service_request_history',sqlEngine,sqlMeta)

print 'All done'
