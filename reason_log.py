import panadas as panda

error_log=panda.read_csv('/home/hema_murthy/testModel/error_log.csv')
products=panda.read_csv('/home/hema_murthy/testModel/testDataSource/asset_daily_work.csv')


#### some error in series! :(

df=panda.DataFrame(['prod_id','reason0','reason1','reason2','reason3','reason04','reason5',])
for prod in products.customer_asset_identifier:
  at_focus=error_log.loc[error_log['customer_asset_identifier']==prod]
  if at_focus.empty: #True - selected
     entry=[prod,1,'0','0,0,0,0]   
  else: #False -in error_log- not selected
     if at_focus[reason_id]==1:
        entry=[prod,0,1,0,0,0,0] 
     elif at_focus[reason_id]==2:
        entry=[prod,0,1,0,0,0,0] 
     elif at_focus[reason_id]==3:
        entry=[prod,0,1,0,0,0,0] 
     elif at_focus[reason_id]==4:
        entry=[prod,0,1,0,0,0,0] 
     elif at_focus[reason_id]==5:
        entry=[prod,0,1,0,0,0,0] 
  df.append(entry)
  
print df.head(5)
