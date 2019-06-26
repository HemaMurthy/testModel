# -*- coding: utf-8 -*-
#!/usr/bin/env python
import numpy as np
import pandas as pd
from datetime import date
from datetime import datetime, timedelta
import time
import sys
import os
#TODO: Break down the fact creating function into more functional and modular functions

def build_facts_product(product_id, notes, products, sr, prod_dfs, max_Date, productError, listOfCombinations, firmwareHistory):
    try:
        notes_in_prod = notes.xs(product_id, level='asset_id').Workunits
        notes_in_prod = notes_in_prod[~notes_in_prod.index.duplicated(keep='last')]
        prod_install_date = products.asset_installed_date[products.asset_id == product_id].values[0]

        firmwareHistoryFiltered = firmwareHistory[firmwareHistory.asset_id == product_id][['asset_life_event_type_value', 'asset_life_event_date']]
        firmwareHistoryFiltered.asset_life_event_date = pd.to_datetime(firmwareHistoryFiltered.asset_life_event_date)
        firmwareHistoryFiltered.sort_values(['asset_life_event_date'], inplace = True)
        firmwareHistoryFiltered['asset_life_event_date'] = pd.to_datetime(firmwareHistory.asset_life_event_date).dt.date
        firmwareHistoryFiltered.set_index('asset_life_event_date', drop=True, inplace=True)
        # Removing duplicates and keeping the last firmware that was installed for that date
        firmwareHistoryFiltered = firmwareHistoryFiltered[~firmwareHistoryFiltered.index.duplicated(keep='last')]

        errorCodes = productError[productError['asset_id'] == product_id].copy()
        errorCodes.date = pd.to_datetime(errorCodes['event_date']).dt.date

        # define start date based on the install date
        start_date = max(notes_in_prod.index.min().date(),
                     prod_install_date)

        # define end date to be the max_date until when the data is available
        end_date = max_Date
        days_since_install = (start_date - prod_install_date).days

        # Reindex to fill in for missing Dates
        notes_in_prod = notes_in_prod.reindex(pd.date_range(start_date, end_date), fill_value=0)

        #Array for cummulative sum between 2 PM Dates
        valCummSumNotes = notes_in_prod.copy()

        #Notes in last 30 Days
        notes_in_prod_last30 = 	notes_in_prod.rolling(min_periods=0,window=30,center=False).sum()

        # UPDATE notes_in
        notes_in_prod = notes_in_prod.cumsum()  # create notes_in values as cumulative sum

        # Calculating the firmware version for each day. The backfill at the end is to handle the null if firmware history record is of a later date
        firmwareHistoryFiltered = firmwareHistoryFiltered.reindex(pd.date_range(start_date, end_date), method = 'ffill').bfill()
        # UPDATE Preventive Maintenance values
        pm_dates = sr.service_request_date[(sr.asset_id == product_id) &
                                   (sr.Incident_Category == 'Preventative Maintenance') &
                                   #(sr.service_request_date >= start_date) &
                                   (sr.incident_date >= prod_install_date) &
                                   (sr.incident_date <= end_date)]



        vals = np.zeros((notes_in_prod.shape[0], 3))
        #Array for days to failure
        failureVals = np.full((notes_in_prod.shape[0], 1), 180)
        #To Store the diffence between the Prod Install Date and the Actual Date from which note were added
        iniBias = (start_date - (prod_install_date if not len(pm_dates) or pm_dates.min() > start_date else pm_dates[pm_dates <= start_date].max())).days
        #iniBias = (start_date - prod_install_date).days
        pm_dates = pm_dates[pm_dates >= start_date]

        seq_start_idx = 0
        seq_start_date = start_date
        for pm_date in pm_dates.sort_values():
            seq_end_idx = (pm_date - start_date).days
            vals[seq_start_idx:seq_end_idx, 0] = range(iniBias, seq_end_idx - seq_start_idx + iniBias)
            valCummSumNotes.ix[seq_start_date:(pm_date - timedelta(days=1))] = valCummSumNotes.ix[seq_start_date:(pm_date - timedelta(days=1))].cumsum()
            seq_start_idx = seq_end_idx
            seq_start_date = pm_date
            #Setting the difference to 0 so that the Consequent Days Since PM Calculation is not affected
            iniBias = 0
        valCummSumNotes.ix[seq_start_date:end_date] = valCummSumNotes.ix[seq_start_date:end_date].cumsum()
        vals[seq_start_idx: len(vals), 0] = range(iniBias, len(vals) - seq_start_idx + iniBias)

        # Updating failure Event Maintenance values
        incidenceNotInterest = ['Preventative Maintenance', 'Log Collection/Configuration', 'Installation/Integration', 'Cleaning Supplies', 'Misrouted Call',  'Welcome Call', 'Serial Number Wrong']
        sr_dates = sr.service_request_date[(sr.asset_id == product_id) &
                                       (~sr.incident_Category.isin(incidenceNotInterest)) &
                                       (~sr.incident_Category.isnull()) &
                                       (sr.incident_date >= prod_install_date) &
                                       (sr.incident_date <= end_date)]

        #Days since Failure
        daysSinceFailure = np.zeros((notes_in_prod.shape[0], 1))
        #Storing the number of incidents that happened - Accounting for incidents that happened before first note was put in
        numServiceRequest = sr_dates[sr_dates < start_date].count()
        iniBias = (start_date - (prod_install_date if not len(sr_dates) or sr_dates.min() > start_date else sr_dates[sr_dates <= start_date].max())).days
        sr_dates = sr_dates[sr_dates >= start_date]
        seq_start_idx = 0
        for sr_date in sr_dates.sort_values():
            start_idx = max(0, (sr_date - start_date).days - 30)
            end_idx = (sr_date - start_date).days
            vals[range(start_idx, end_idx + 1), 1] = 1
            vals[seq_start_idx:end_idx, 2] = [numServiceRequest] * (end_idx - seq_start_idx)

            daysSinceFailure[seq_start_idx:end_idx, 0] = range(iniBias, end_idx - seq_start_idx + iniBias)
            #Days to failure calculation
            start_idx_df = max(0, (sr_date - start_date).days - 180)
            for i in xrange(start_idx_df, end_idx + 1):
                failureVals[i] = min(failureVals[i], end_idx - i)
            numServiceRequest += 1
            seq_start_idx = end_idx
            iniBias = 0

        vals[seq_start_idx:, 2] = [numServiceRequest] * (len(vals) - seq_start_idx)
        daysSinceFailure[seq_start_idx: len(vals), 0] = range(iniBias, len(vals) - seq_start_idx + iniBias)

        prod_df = pd.DataFrame(data={'customer_asset_identifier': product_id,
                                 'notes_in': notes_in_prod,
                                 'notes_in_last30': notes_in_prod_last30,
                                 'notes_in_last_pm': valCummSumNotes,
                                 'last_pm_date': vals[:, 0],
                                 'age': range(0 + days_since_install, len(vals) + days_since_install),
                                 'firmwareHistory': firmwareHistoryFiltered['firmware_version_id'],
                                 'failure_event': vals[:, 1],
                                 'day_to_failure': failureVals[:, 0],
                                 'numIncidents': vals[:, 2],
                                 'days_Since_failure': daysSinceFailure[:, 0]
                                 })
        prod_df['Avg_notes_in'] = prod_df['notes_in'].astype(float) / prod_df['age']
        prod_df['Avg_notes_in_last30'] = prod_df['notes_in_last30'].astype(float) / 30.0

        # Starting with the error code count
        for codeType in listOfCombinations['codeType']:
            for severity in listOfCombinations['codeSeverity']:
                errorCodesCategory = errorCodes[(errorCodes.event_code_type_id == codeType) & (errorCodes.event_code_criticality == severity) & (errorCodes.event_code_value not in listOfCombinations['highSeverityCodes']].groupby('event_date').count()
                errorCodeCount = errorCodesCategory.reindex(pd.date_range(start_date, end_date), fill_value=0)
                errorCodeStd = errorCodeCount['event_code_type_id'].rolling(window=30,center=False).std()
                errorCodeCount = errorCodeCount.cumsum()
                last30DayErrorCount = errorCodeCount.event_code_type_id.subtract(errorCodeCount.event_code_type_id.shift(30), fill_value = 0)
                prod_df['CodeType_'+str(int(codeType)) + '_criticality_'+str(int(severity))] = last30DayErrorCount
                prod_df['CodeType_'+str(int(codeType)) + '_criticality_'+str(int(severity)) + '_std'] = errorCodeStd

        #Now for the high error issues:
        for errorCode in listOfCombinations['highSeverityCodes']:
            errorCodesCategory = errorCodes[(errorCodes.event_code_value == errorCode)].groupby('event_date').count()
            errorCodeCount = errorCodesCategory.reindex(pd.date_range(start_date, end_date), fill_value=0)
            errorCodeStd = errorCodeCount['event_code_type_id'].rolling(window=30,center=False).std()
            errorCodeCount = errorCodeCount.cumsum()
            last30DayErrorCount = errorCodeCount.event_code_type_id.subtract(errorCodeCount.event_code_type_id.shift(30), fill_value = 0)
            prod_df['Code_'+str(int(errorCode))] = last30DayErrorCount
            prod_df['Code_'+str(int(errorCode)) + '_std'] = errorCodeStd

        #Fetching the device information
        deviceFeature = products[products.asset_id == product_id][['model_name', 'model_group_id', 'model_group_name', 'category_id', 'category_name', 'type_id', 'type_name', 'capacity', 'security_type']]

        for column in deviceFeature.columns:
            prod_df[column] = deviceFeature[column].values[0]
        # Adding this DF to the global list
        prod_dfs.append(prod_df[:])
    except Exception as e:
        print 'There was an error while processing for product id', product_id, e.message
        raise


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

if __name__ == "__main__":
    DATADIR = 'DataSource'
    # READ DATA INPUT FROM CSV FILES
    # Reading the assest daily work to get the note summary data
    notes = pd.read_csv(os.path.join(DATADIR, 'dailywork.csv'))
    notes['DailyWork_date'] = pd.to_datetime(notes['DailyWork_date']).dt.date
    notes.set_index(['asset_id', 'DailyWork_date'], drop=True, inplace=True)



    # Reading the product description table
    products = pd.read_csv(os.path.join(DATADIR,'Asset.csv'))
    products.asset_installed_date = pd.to_datetime(products.asset_installed_date).dt.date

    # Reading the service logs. This helps us in figuring out the days to failure and last Preventive Maintenance date
    sr = pd.read_csv(os.path.join(DATADIR,'incident.csv'))
    sr.incident_date = pd.to_datetime(sr.incident_date).dt.date
    # List of product ids that have the following conditions satisfied
    '''
     a. The Install Date is after 2010
     b. Data is present for atleast 60 days
     c. Log Status remains 1 throught the product's life
    '''
    productStatus = pd.read_csv(os.path.join(DATADIR,'APIProductList.csv'), header=None)
    productStatus.columns = ['id']

    productError = pd.read_csv(os.path.join(DATADIR,'asset_event.csv')).dropna()

    # Reading the device description table
    #assestFeatures = pd.read_csv(os.path.join(DATADIR,'asset_features.csv'))

    # Reading the firmware history table
    firmwareHistory = pd.read_csv(os.path.join(DATADIR,'asset_life_event.csv'))
    # Removing rows for which we have no information for the created date column
    firmwareHistory = firmwareHistory[(~firmwareHistory['asset_life_event_date'].isnull())]
    firmwareHistory = firmwareHistory[(~firmwareHistory['asset_life_event_date'].str.contains('0000-00-00 00:00:00'))]
    firmwareHistory = firmwareHistory[(firmwareHistory['asset_life_event_type_value'] == 4)]

    #Considering all the products that have 90 days of data
    lastDate = datetime.now() - timedelta(days=90)
    #productStatus['Install Date'] = pd.to_datetime(productStatus['Install Date']).dt.date

    #Filter out the products newer than last date. We will only consider these products
    #productIdFiltered = productStatus[productStatus['Install Date'] < lastDate.date()]
    max_Date = datetime.now().date()

    #Caculating the different Categories of error codes
    #Hardcoding the stuff rightnow as we don't have a lot of these with data
    combinations = {
    'codeType': [1.0],
    'codeSeverity': [1.0, 2.0, 3.0],
    'highSeverityCodes': [160, 177,  12,  74,  64, 182]
    }

    # The list to store the DF for each of the product
    prod_dfs = []
    for i, product_id in enumerate(productStatus['id']):
        print_progress(i, len(productStatus['id']))
        try:
            build_facts_product(product_id, notes, products, sr, prod_dfs, max_Date, productError, combinations, firmwareHistory)
        except:
            print 'Skipping fact creation product_id', product_id
            raise
        time.sleep(0.001)
    complete_data = pd.concat(prod_dfs)
    complete_data.index.name = 'date'
    complete_data.set_index(['customer_asset_identifier'], append=True, drop=True, inplace=True)
    complete_data.sort_index(inplace=True)
    complete_data.to_csv('Facts-Data.csv', index=True, index_label=['date', 'customer_asset_identifier'])
