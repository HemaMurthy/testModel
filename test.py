import pandas as panda
import sys
import os

def read_from_csv_files():
   try:
        data_dir='/home/hema_murthy/testModel/testDataSource'
        notes=panda.read_csv(os.path.join(data_dir,'asset_daily_work.csv'))
        notes['date'] = panda.to_datetime(notes['date']).dt.date
        notes.set_index(['customer_asset_identifier','date'],drop=True,inplace=True)
        #notes contains col1:customer_asset_id and col2: date

        products=panda.read_csv(os.path.join(data_dir,'asset_ib_info.csv'))
        products.installed_date=panda.to_datetime(products.installed_date).dt.date
        product_status=products['active_status']
        product_status.append(products['customer_asset_identifier'])

        asset_features= panda.read_csv(os.path.join(data_dir,'asset_features.csv'))
        condition_events=panda.read_csv(os.path.join(data_dir,'asset_conditions_events.csv'))
        firmware_history=panda.read_csv(os.path.join(data_dir,'firmware_history.csv'))
        return products,notes,product_status,asset_features,condition_events,firmware_history
   except Exception as e:
        print 'Error in reading data from csv file ', e.message
        raise

if __name__ == "__main__":

        #reading csv files
        products,notes,product_status,asset_features,condition_events,firmware_history=read_from_csv_files()
        
~                                                                                                                                                    
~                                                                                                                                                    
~                                                                    
