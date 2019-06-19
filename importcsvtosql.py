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
DATADIR = 'C:/Users/pooja/OneDrive/Desktop/eKryp' #change to the necessary directory 

#alter this to match the table structure
def getTableSchema(tableName, meta):
    if tableName == 'Asset_information': #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('customer_asset_identifier',Integer, nullable=False),
                Column('install_date',String(10), nullable=False),
                Column('model_name', String(45), nullable=False),
                Column('model_group_id', Integer, nullable=False),
                Column('category_id', Integer, nullable=False),
                Column('type_id', Integer, nullable=False),
                Column('capacity', Integer, nullable=False),
                Column('attribute reference', String(45), nullable=False),
                Column('ekryp_customer_id',Integer, nullable=False),
                Column('Asset_serial_number',String(45), nullable=False),
                Column('location_name', String(45), nullable=False),
                Column('end_customer',String(45), nullable=False),
                Column('service_provider', String(45), nullable=False),
        )
def pushToSQL(tableSchema, df, tableName, engine, meta):
    print('Pushing population parameters to MY SQL')
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)
#load table info from ekryp_data_db_prod!!! i've already done that, and i have it on my GCP directory
dummy=panda.read_csv(os.path.join(DATADIR,'Asset_information.csv'))

#alter this to fit the info in df to match the columns in table
df = panda.DataFrame({
    'customer_asset_identifier': dummy['customer_asset_identifier'],
    'install_date': dummy['install_date'],
    'model_name':dummy['model_name'],
    'model_group_id': dummy['model_group_id'],
    'category_id':dummy['category_id'],
    'type_id':dummy['type_id'],
    'capacity':dummy['capacity'],
    'attribute_reference':dummy['attribute_reference'],
    'ekryp_customer_id':dummy['ekryp_customer_id'],
    'Asset_serial_number':dummy['Asset_serial_number'],
    'location_name':dummy['location_name'],
    'end_customer' :dummy['end_customer'],
    'service_provider': dummy['service_provider']
})

#table you want to populate
tablename='Asset_information'

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema(tablename,sqlMeta)
pushToSQL(tableSchema,df,'Asset_information',sqlEngine,sqlMeta)

print('All done')