import pandas as panda
import sys
import os

class dataClass:
   def __init__(self,products,notes,product_status,asset_features,condition_events,firmware_history):
      self.notes_summary=notes
      self.products=products
      self.product_status=product_status
      self.asset_features=asset_features
      self.condition_events=condition_events
      self.firmware_history=firmware_history
    
   def cleanup():
        #performs cleanup - removes null, or rows with missing info
         print "hey it works"
         firmwareHistory = firmwareHistory[(~firmwareHistory['created_date'].isnull())]
         firmwareHistory = firmwareHistory[(~firmwareHistory['created_date'].str.contains('0000-00-00 00:00:00'))]
  
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
        condition_events=panda.read_csv(os.path.join(data_dir,'asset_conditions_events.csv')) #contains error code info
        firmware_history=panda.read_csv(os.path.join(data_dir,'firmware_history.csv'))
        return products,notes,product_status,asset_features,condition_events,firmware_history
   except Exception as e:
        print 'Error in reading data from csv file ', e.message
        raise

def filter1(data):
   #dat
      
if __name__ == "__main__":

        #reading csv files
        products,notes,product_status,asset_features,condition_events,firmware_history=read_from_csv_files()
        data=dataClass(products,notes,product_status,asset_features,condition_events,firmware_history);
        data.cleanup()
         
        data=filter1(data,2010) #to select install date after 2010
        data=filter2(data,60) #to make sure asset data is present for atleast 60 days
        data=filter3(data) #to select the asset whose log status is 1
~                                                                                                                                                    
~                                                                                                                                                    
~                                                                    
