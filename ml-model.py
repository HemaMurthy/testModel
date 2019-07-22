#!/usr/bin/env python
'''
This file reads the fact data and generates the models
'''
import pandas as pd
import numpy as np
import sys
import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from datetime import datetime, timedelta
from sklearn.externals import joblib
from sklearn.metrics import roc_auc_score

import pdb

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

import json

ModelResultTable = 'ModelResults'
RocCurveTable = 'RocCurve'
PrecisionCurveTable = 'PrecisionCurve'


def prec_curve(y_true,y_score):
    n_total  = len(y_true)
    n_device = np.zeros(n_total)
    prec = np.zeros(n_total)
    temp = [x for _,x in sorted(zip(y_score,y_true),reverse=True)]
    for j in range(n_total):
        n_device[j] = j+1
        prec[j] = np.mean(temp[0:j+1])

    return n_device,prec

def generatePrecisionCurve(rpt_data, date, failure_event_col,mdlName, mdlVersion):

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
        plt.savefig('m4__precision_Curve.png')

    
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

def generateROC(rpt_data, date, failure_event_col, mdlName,mdlVersion):
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
            
        print 'ROC_AUC : ',roc_auc_score(use_data[failure_event_col],use_data['mdl_pred'])    
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
        plt.savefig('m4__ROC_Curve.png')


def test_train_split(df, trainMaxDate, testMaxDate):
    trainDf = df[(df.date <= trainMaxDate)].copy()
    testDf = df[(df.date > trainMaxDate) & (df.date <= testMaxDate)].copy()
    return trainDf, testDf

def run_model(testData, trainData, mdl, predCol, mdlName, mdlVersion,
              outputTransform = None):
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
    generateROC(rpt_data, testMaxDate, predCol, mdlName, mdlVersion)
    generatePrecisionCurve(rpt_data, testMaxDate, predCol, mdlName, mdlVersion)
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

MODELTRAINDATE='2019-01-26'

trainMaxDate = datetime.strptime(MODELTRAINDATE.strip('"'), '%Y-%m-%d').date()
testMaxDate = datetime.strptime(MODELTRAINDATE.strip('"'), '%Y-%m-%d') + timedelta(30)
minDate = '2015-01-01'
data = pd.read_csv('m4Facts-Data.csv')
data.date = pd.to_datetime(data.date)
dataReduced = data.drop([u'model_group_name', u'category_name', u'type_name'], 1)

securityTypeMap = pd.read_csv('/home/hema_murthy/testModel/TestDataSource/SecurityTableEkryp.csv')
modelNameMap = pd.read_csv('/home/hema_murthy/testModel/TestDataSource/ModelTableEkryp.csv')
#One hot encoding

dataReduced['security_type'] = dataReduced['security_type'].map(securityTypeMap.set_index('security_type').to_dict()['id'])
dataReduced['model_name'] = dataReduced['model_name'].map(modelNameMap.set_index('model_name').to_dict()['id'])

dataReduced = dataReduced[(dataReduced.date >= datetime.strptime(minDate, '%Y-%m-%d')) & (dataReduced.date <= testMaxDate)]
dataReduced.replace([np.inf, -np.inf], np.nan, inplace=True)
dataReduced.fillna(0, inplace=True)
classificationData = dataReduced.drop(['day_to_failure'], 1)

train, test = test_train_split(classificationData, trainMaxDate, testMaxDate)
print('Length of train',len(train))
print('Length of test',len(test))

classificationModels = [
        ('RandomForestClassifier(n_estimators=100,max_depth=7,class_weight=balanced,max_features=sqrt)',
         RandomForestClassifier(n_estimators=100,max_depth=7,class_weight="balanced",max_features="sqrt"))
]
ColsToDrop = [u'customer_asset_identifier', u'date', 'failure_event']
results = []
modelNames = []
mdlVersion = '1.02'

for mdlName, mdl in classificationModels:
    print ('Running for Model', mdlName)
    mdlResult = run_model(test, train, mdl, 'failure_event', mdlName, mdlVersion)

    mdl.feature_columns = list(train.drop(ColsToDrop, 1).columns)
    mdl.eKrypVersion = mdlVersion
    mdl.trainDate = str(datetime.now().date())

    joblib.dump(mdl, mdlName + '_m4_' + str(trainMaxDate) + '.pkl', compress =1)
    modelNames.append(mdlName)
    results.append(mdlResult)

#We need to send these results automatically to Jay, Ramki and Steve for verification
re = pd.DataFrame(results)
re['Modelname'] = pd.Series(modelNames)
re.to_csv('ModelTrainingResults_m4'+ str(trainMaxDate)+'.csv', index = False)

