
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
DATADIR = '/home/hema_murthy/testModel/TestDataSource' #change to the necessary directory 

#alter this to match the table structure
def getTableSchema(tableName, meta):
    if tableName == technician_history: #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('ekryp_customer_id', Integer, nullable=False),
                Column('customer_asset_identifier',Integer, nullable=False),
                Column('asset_serial_number', String(15), nullable=False),
                Column('serial_order_date',String(10), nullable=False),
                Column('service_provider', Integer, nullable=False),
                Column('technician_id', Integer, nullable=False),
                Column('technician_name', String(30), nullable=False),
                Column('technician_arrival_date',String(30),nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)

#load table info from ekryp_data_db_prod!!! i've already done that, and i have it on my GCP directory
sr=panda.read_csv(os.path.join(DATADIR,'service_order_record.csv'))
products=panda.read_csv(os.path.join(DATADIR,'asset_ib_info.csv'))
field_tech=panda.read_csv(os.path.join(DATADIR,'field_service_ops_activity_record.csv'))

sr=field_tech.join(sr.set_index('id'),how='inner',on='field_service_order_id',lsuffix='new')
sr=sr.join(products.set_index('customer_asset_identifier'),on='customer_asset_identifier',lsuffix='new')

#sr=sr.join(products,on='customer_asset_identifier',how='inner',lsuffix='customer_asset_identifier')

#alter this to fit the info in df to match the columns in table
df = panda.DataFrame({
    'ekryp_customer_id':1,
    'customer_asset_identifier': sr['customer_asset_identifier'],
    'asset_serial_number':sr['serial_number'],
    'serial_order_date': sr['service_order_created_on'],
    'service_provider':sr['service_provider'],
    'technician_id':sr['field_technician_id'],
    'technician_name': sr['field_technician'],
    'technician_arrival_date':sr['date_arrival']
})
df=df.astype({'technician_id':int})
#table you want to populate
technician_history='technician_history'

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema(technician_history,sqlMeta)
pushToSQL(tableSchema,df,technician_history,sqlEngine,sqlMeta)

print 'All done'
