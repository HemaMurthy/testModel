#!/usr/bin/env python
'''
This file reads the fact data and generates the models
'''
import pandas as pd
import numpy as np
import sys
from matplotlib import pyplot as plt
from datetime import datetime, timedelta
from sklearn.externals import joblib

#For Classification models
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
#from xgboost import XGBClassifier, XGBRegressor
# Classification Metrics
from sklearn.metrics import recall_score, precision_score, accuracy_score, confusion_matrix, roc_curve, auc
#import lightgbm as lgb
np.random.seed(42)
import seaborn as sns
from matplotlib import pyplot as plt
from scipy.stats import percentileofscore
#SQL Related imports
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float
from sqlalchemy.sql import exists
import json

ModelResultTable = 'ModelResults'
RocCurveTable = 'RocCurve'
PrecisionCurveTable = 'PrecisionCurve'

def getTableSchema(tableName, meta):
    if tableName == ModelResultTable:
        return Table(tableName, meta,
                Column('ModelName', String(250), nullable=False),
                Column('ModelVersion', String(10), nullable=False),
                Column('train_date', String(10), nullable=False),
                Column('test_start', String(10), nullable=False),
                Column('test_end', String(10), nullable=False),
                Column('curve_date', String(10), nullable=False),
                Column('precision', Float, nullable=False),
                Column('recall', Float, nullable=False),
                Column('accuracy', Float, nullable=False),
                Column('costMatrix', Float, nullable=False)
        )
    elif tableName == RocCurveTable:
        return Table(tableName, meta,
                Column('ModelName', String(250), nullable=False),
                Column('ModelVersion', String(10), nullable=False),
                Column('train_date', String(10), nullable=False),
                Column('pred_x', Float, nullable=False),
                Column('pred_y', Float, nullable=False),
                Column('base_x', Float, nullable=False),
                Column('base_y', Float, nullable=False)
        )
    elif tableName == PrecisionCurveTable:
        return Table(tableName, meta,
                Column('ModelName', String(250), nullable=False),
                Column('ModelVersion', String(10), nullable=False),
                Column('train_date', String(10), nullable=False),
                Column('prec_mdl_x', Float, nullable=False),
                Column('prec_mdl_y', Float, nullable=False),
                Column('prec_base_x', Float, nullable=False),
                Column('prec_base_y', Float, nullable=False)
        )

def getSQlEngine():
    with open("db_creds.json") as f:
        dbCreds = json.load(f)

    PASSWORD = dbCreds['password']
    DB_NAME = dbCreds['db_name_ml']
    HOST = dbCreds['host']
    USER = dbCreds['user']
    engine = create_engine("mysql+mysqldb://" + USER + ":"+PASSWORD+"@" + HOST + "/"+DB_NAME)
    meta = MetaData(bind=engine)
    return engine, meta

def pushToSQL(tableSchema, df, tableName, engine, meta):
    print 'Pushing population parameters to MY SQL'
    ## TABLE TO PUSH###
    table_pop_parameters = tableSchema
    meta.create_all(engine)
    df.to_sql(tableName, engine, if_exists= 'append', index=False)

def prec_curve(y_true,y_score):
    n_total  = len(y_true)
    n_device = np.zeros(n_total)
    prec = np.zeros(n_total)
    temp = [x for _,x in sorted(zip(y_score,y_true),reverse=True)]
    for j in range(n_total):
        n_device[j] = j+1
        prec[j] = np.mean(temp[0:j+1])

    return n_device,prec

def generatePrecisionCurve(rpt_data, date, failure_event_col,
                           mdlName, mdlVersion, sqlEngine, sqlMeta):
    mdl_cut = [('red',0.74),('orange',0.70),('yellow',0.65)]
    bl_cut = [('red',130000),('orange',100000)]  #,('yellow',80000)]
    use_data = rpt_data[rpt_data.date == date]
    if len(use_data):
        nd1,prec1 = prec_curve(use_data[failure_event_col],use_data['mdl_pred'])
        nd2,prec2 = prec_curve(use_data[failure_event_col],use_data['pctl_bl'])
        fpct = use_data[failure_event_col].mean()
        sns.set_style('darkgrid')
        m = 100
        # Model
        plt.plot(nd1[:m],prec1[:m],color='green', lw=2,label='Model')
        # Baseline
        plt.plot(nd2[:m],prec2[:m],color='lightcoral',lw=2,label='Baseline')
        # Random
        plt.plot([0,m],[fpct,fpct],color='navy',lw=2,linestyle='--',label='Random')
#        plt.plot([0,m],[0.5,0.5],color='gray',lw=2,linestyle='--',label='50%')

        title = 'Prediction Date: ' + date.strftime('%Y-%m-%d') + '   30-day results'
        plt.xlabel('# Predictions')
        plt.ylabel('Precision')
        plt.title(title,fontweight='bold')
        plt.legend(loc='upper right')
        plt.savefig(mdlName + '_precision_Curve.png')

        #Saving the values to SQL Database
        tableSchema = getTableSchema(PrecisionCurveTable, sqlMeta)
        df = pd.DataFrame({
            'ModelName': mdlName,
            'ModelVersion': mdlVersion,
            'train_date': str(datetime.now().date()),
            'prec_mdl_x': nd1[:m],
            'prec_mdl_y': prec1[:m],
            'prec_base_x': nd2[:m],
            'prec_base_y': prec2[:m]
        })
        pushToSQL(tableSchema, df, PrecisionCurveTable, sqlEngine, sqlMeta)

'''
Functions for generating the ROC curves
'''
def getProductData(data, date_list, var_list):
    data = data[['date','customer_asset_identifier', 'failure_event']+var_list]
    data.date = pd.to_datetime(data.date)
    data = data[data.date.isin(date_list)]
    return data

def getPredData(data, date_list):
    col_name_repl = {'failure_probability':'mdl_pred'}
    data.rename(columns=col_name_repl, inplace=True)
    data = data[['date','customer_asset_identifier','mdl_pred']]
    data.date = pd.to_datetime(data.date)
    data = data[data.date.isin(date_list)]
    return data

def lor_curve(y_true,y_score):
    n_total  = len(y_true)
    n_device = np.zeros(n_total)
    n_fail   = np.zeros(n_total)
    temp = [x for _,x in sorted(zip(y_score,y_true),reverse=True)]
    for j in range(n_total):
        n_device[j] = 1. * j / n_total
        n_fail[j] = 1. * sum(temp[0:j+1]) / sum(temp)

    return n_device,n_fail

def generateROC(rpt_data, date, failure_event_col, mdlName,
                mdlVersion, sqlEngine, sqlMeta):
    mdl_cut = [('red',0.74),('orange',0.70),('yellow',0.65)]
    bl_cut = [('red',130000),('orange',100000)]  #,('yellow',80000)]

    rpt_data['pctl_bl'] = rpt_data.groupby(['date'])['notes_in_last_pm'].rank()
    rpt_data['pctl_bl'] = rpt_data['pctl_bl'] / rpt_data['pctl_bl'].max()
    use_data = rpt_data[rpt_data.date == date]
    if len(use_data):
        nd1,nf1 = lor_curve(use_data[failure_event_col],use_data['mdl_pred'])
        nd2,nf2 = lor_curve(use_data[failure_event_col],use_data['pctl_bl'])
        fpct = use_data[failure_event_col].mean()
        sns.set_style('darkgrid')

        # Ideal
        plt.plot([0,fpct],[0,1],color='gray',lw=2,linestyle='--')
        plt.plot([fpct,1],[1,1],color='gray',lw=2,linestyle='--',label='Ideal')
        # Model
        plt.plot(nd1,nf1,color='green', lw=2,label='Model (%0.2f)'%(auc(nd1,nf1)-0.5))
        # Baseline
        plt.plot(nd2,nf2,color='lightcoral',lw=2,label='Baseline (%0.2f)'%(auc(nd2,nf2)-0.5))
        # Random
        plt.plot([0,1],[0,1],color='navy',lw=2,linestyle='--',label='Random')

        # Model cutoffs
        for (c,val) in mdl_cut:
            pval = percentileofscore(use_data['mdl_pred'],val)
            px,py = [np.percentile(nd1,100-pval),np.percentile(nf1,100-pval)]
            plt.plot(px,py,marker='o',color=c)
        # Baseline cutoffs
        for (c,val) in bl_cut:
            pval = percentileofscore(use_data['notes_in_last_pm'],val)
            px,py = [np.percentile(nd2,100-pval),np.percentile(nf2,100-pval)]
            plt.plot(px,py,marker='o',color=c)

        title = 'Prediction Date: ' + date.strftime('%Y-%m-%d') + '   30-day results'
        plt.xlabel('# Devices')
        plt.ylabel('# Failures')
        plt.title(title,fontweight='bold')
        plt.legend(loc='lower right')
        plt.savefig(mdlName + '_ROC_Curve.png')

        #Saving the results to a SQL database table
        tableSchema = getTableSchema(RocCurveTable, sqlMeta)
        df = pd.DataFrame({
            'ModelName': mdlName,
            'ModelVersion': mdlVersion,
            'train_date': str(datetime.now().date()),
            'pred_x': nd1,
            'pred_y': nf1,
            'base_x': nd2,
            'base_y': nf2
        })
        pushToSQL(tableSchema, df, RocCurveTable, sqlEngine, sqlMeta)


def test_train_split(df, trainMaxDate, testMaxDate):
    trainDf = df[(df.date <= trainMaxDate)].copy()
    testDf = df[(df.date > trainMaxDate) & (df.date <= testMaxDate)].copy()
    return trainDf, testDf

def run_model(testData, trainData, mdl, predCol, mdlName, mdlVersion,
              sqlEngine, sqlMeta, outputTransform = None):
    mdl.fit(trainData.drop(['date', 'customer_asset_identifier'] + [predCol], 1), trainData[predCol])
    pred_y = mdl.predict(testData.drop(['date', 'customer_asset_identifier'] + [predCol], 1))
    prob_y = mdl.predict_proba(testData.drop(['date', 'customer_asset_identifier'] + [predCol], 1))
    if outputTransform is not None:
        pred_y = outputTransform(pred_y)

    #Genrating the ROC Curve#
    prod_data = getProductData(testData, [testMaxDate], ['notes_in_last_pm'])
    preddf = pd.DataFrame({
        'date': testData['date'],
        'failure_probability': prob_y[:,1],
        'customer_asset_identifier': testData['customer_asset_identifier']
    })
    pred_data = getPredData(preddf, [testMaxDate])
    rpt_data = pd.merge(prod_data, pred_data, how='inner', on=['date', 'customer_asset_identifier'])
    #rpt_data = pd.merge(rpt_data, testData[['customer_asset_identifier', 'date'] + [predCol]], how='left', on=['date', 'customer_asset_identifier'])
    generateROC(rpt_data, testMaxDate, predCol, mdlName, mdlVersion,
                sqlEngine, sqlMeta)
    generatePrecisionCurve(rpt_data, testMaxDate, predCol, mdlName,
                           mdlVersion, sqlEngine, sqlMeta)
    return evaluateModel(testData[predCol], pred_y)

def evaluateModel(true_y, pred_y):
    if len(np.unique(pred_y)) != 2:
        ##Lets Convert the regression result to classification one
        true_y = (true_y <= 30).astype(int)
        pred_y = (pred_y <= 30).astype(int)

    costMatrix = np.array([np.array([0, 1]), np.array([2, -1])])
    confMatrix = confusion_matrix(true_y, pred_y)
    retObj = {
        'recall' : recall_score(true_y, pred_y),
        'precision' : precision_score(true_y, pred_y),
        'accuracy' : accuracy_score(true_y, pred_y),
        'costMatrix' : np.sum(confMatrix*costMatrix)
    }
    return retObj

trainMaxDate = datetime.strptime(sys.argv[1].strip('"'), '%Y-%m-%d').date()
testMaxDate = datetime.strptime(sys.argv[1].strip('"'), '%Y-%m-%d') + timedelta(30)
minDate = '2015-01-01'  #why tho?
data = pd.read_csv('/home/hema_murthy/testModel/Facts-Data.csv')
data.date = pd.to_datetime(data.date)
dataReduced = data.drop([u'model_group_name', u'category_name', u'type_name'], 1)

securityTypeMap = pd.read_csv('SecurityTableEkryp.csv')
modelNameMap = pd.read_csv('ModelTableEkryp.csv')
#One hot encoding

dataReduced['security_type'] = dataReduced['security_type'].map(securityTypeMap.set_index('security_type').to_dict()['id'])
dataReduced['model_name'] = dataReduced['model_name'].map(modelNameMap.set_index('model_name').to_dict()['id'])

dataReduced = dataReduced[(dataReduced.date >= datetime.strptime(minDate, '%Y-%m-%d')) & (dataReduced.date <= testMaxDate)]
dataReduced.replace([np.inf, -np.inf], np.nan, inplace=True)
dataReduced.fillna(0, inplace=True)
classificationData = dataReduced.drop(['day_to_failure'], 1)

train, test = test_train_split(classificationData, trainMaxDate, testMaxDate)

classificationModels = [
        ('RandomForestClassifier(n_estimators=100,max_depth=7,class_weight=balanced,max_features=sqrt)',
         RandomForestClassifier(n_estimators=100,max_depth=7,class_weight="balanced",max_features="sqrt"))
]
ColsToDrop = [u'customer_asset_identifier', u'date', 'failure_event']
results = []
modelNames = []
mdlVersion = '1.02'
sqlEngine, sqlMeta = getSQlEngine()
for mdlName, mdl in classificationModels:
    print 'Running for Model', mdlName
    mdlResult = run_model(test, train, mdl, 'failure_event', mdlName,
                          mdlVersion, sqlEngine, sqlMeta)

    mdl.feature_columns = list(train.drop(ColsToDrop, 1).columns)
    mdl.eKrypVersion = mdlVersion
    mdl.trainDate = str(datetime.now().date())

    joblib.dump(mdl, mdlName + '_' + str(trainMaxDate) + '.pkl')
    modelNames.append(mdlName)
    results.append(mdlResult)

#We need to send these results automatically to Jay, Ramki and Steve for verification
re = pd.DataFrame(results)
re['Modelname'] = pd.Series(modelNames)
re.to_csv('/home/hema_murthy/testModel/TrainingResults'+ str(trainMaxDate)+'.csv', index = False)
'''
#Saving Model Training results to database
tableSchema = getTableSchema(ModelResultTable, sqlMeta)
df = pd.DataFrame({
    'ModelName': re['Modelname'],
    'ModelVersion': mdlVersion,
    'train_date': str(datetime.now().date()),
    'test_start': trainMaxDate,
    'test_end': str(testMaxDate).split(' ')[0],
    'curve_date': str(testMaxDate).split(' ')[0],
    'precision': re['precision'],
    'recall': re['recall'],
    'accuracy': re['accuracy'],
    'costMatrix': re['costMatrix'],
})
pushToSQL(tableSchema, df, ModelResultTable, sqlEngine, sqlMeta)
'''
