
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
    if tableName == 'asset_error_codes': #need to change it to ml-refernence tables
        return Table(tableName, meta,
                Column('ekryp_customer_id', Integer, nullable=False),
                Column('customer_asset_identifier',Integer, nullable=False),
                Column('asset_serial_number', String(15), nullable=False),
                Column('date',String(10), nullable=False),
                Column('code_id', Integer, nullable=False),
                Column('value', Integer, nullable=False)
        )

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)

def createDF(new_df):
    print 'Created new DataFrame.. ready to push into DB..'
    #alter this to fit the info in df to match the columns in table
    df = panda.DataFrame({
        'ekryp_customer_id':1,
        'customer_asset_identifier': new_df['customer_asset_identifier'],
        'asset_serial_number':new_df['serial_number'],
        'date': new_df['date'],
        'code_id': new_df['code_id'], 
        'value': new_df['value']
    })
    return df

#table you want to populate
asset_error_codes='asset_error_codes'

#load table info from ekryp_data_db_prod!!! i've already done that, and i have it on my GCP directory
events=panda.read_csv(os.path.join(DATADIR,'asset_conditions_events.csv'))
#change dtype of date and drop unnecesary columns
events['date'] = panda.to_datetime(events['date']).dt.date
events.drop(columns=['time','code_type','criticality' ,'description'],inplace=True)

#insert value column into events
events.insert(4,'value',0)


#get unique customer_asset_identifiers


i=0
while i<len(events):
 df_events=events.loc[i:i+20000]
 event_list=df_events['customer_asset_identifier'].drop_duplicates()
 #create new df with columns as same as events
 new_df=panda.DataFrame(columns=events.columns)

 #run a loop for each customer_asset_identifer
 for prod in event_list:
        #get all events that happened for a particular prod
        prod=events[(events['customer_asset_identifier']==prod)]
        #get all the dates for that prod
        dates=list(prod['date'].drop_duplicates())
        #run a loop for all the dates for that prod
        for d in dates: 
                #drop duplicates for the events in that date
                code_prod=prod[(prod['date']==d)].drop_duplicates()
                #get unique codes that happened that day
                un_codes=list(code_prod['code_id'].drop_duplicates())
                #run a loop for all the codes that happened that day for that prod
                for c in range(0,len(un_codes)):
                  #get row data of that code
                  x_codes=code_prod[(code_prod['code_id']==un_codes[c])]
                  #count the times it occured that day
                  val=(x_codes.size/7)
                  #assign that value to row entry's value
                  x_codes.iat[0,4]=val
                  #append this into new df
                  new_df=new_df.append(x_codes.iloc[0])
 print 'created new df for ',i,' rows successfully'
 df=createDF(new_df)  
 sqlEngine, sqlMeta = getSQlEngine()
 tableSchema= getTableSchema(asset_error_codes,sqlMeta)
 pushToSQL(tableSchema,df,asset_error_codes,sqlEngine,sqlMeta)
 i+=20000
 print 'pushing ',i,' rows..'
   
print 'All done'
