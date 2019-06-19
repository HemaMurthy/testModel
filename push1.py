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
DATADIR = 'C:/Users/ajith/Downloads/populating_db'

#alter this to match the table structure
def getTableSchema(tableName, meta):
    if tableName == 'error_code_feature_def': #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('ekryp_customer_id',Integer, nullable=False),
                Column('code_id',Integer, nullable=False),
                Column('priority', Integer, nullable=False),
                Column('code_type', Integer, nullable=False),
                Column('description', String(100), nullable=False),
                Column('machine_code', String(15), nullable=False),
                Column('include_in_priority_group',Integer, nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)


dummy=panda.read_csv(os.path.join(DATADIR,'error_code_feature_def.csv'))

#alter this to fit the info in df to match the columns in table
df = panda.DataFrame({
    'ekryp_customer_id': dummy['ekryp_customer_id'],
    'code_id': dummy['code_id'],
    'priority':dummy['priority'],
    'code_type': dummy['code_type'],
    'description':dummy['description'],
    'machine_code':dummy['machine_code'],
    'include_in_priority_group':dummy['include_in_priority_group'],
    
})

#table you want to populate
tablename='error_code_feature_def'

sqlEngine, sqlMeta = getSQlEngine()
tableSchema= getTableSchema(tablename,sqlMeta)
pushToSQL(tableSchema,df,'error_code_feature_def',sqlEngine,sqlMeta)

print 'All done'