
import pandas as panda
import numpy as np

import asset_report

notes=panda.read_csv('/home/hema_murthy/testModel/TestDataSource/asset_daily_work.csv')
prod_info=panda.read_csv('/home/hema_murthy/testModel/TestDataSource/asset_ib_info.csv')
prod=prod_info.customer_asset_identifier
products=np.array(prod,dtype=int)

notes_prod=list(notes['customer_asset_identifier'].drop_duplicates())
prod_list=np.zeros([len(prod),13],dtype=int)
prod_list=np.insert(prod_list,0,prod,axis=1)

avail_notes,valid_list,error_log_list,install_date_error_list,notes_60,no60_notes,no_notes_list,perfect_notes,daily_alone_list,daily_list,man_list=asset_report.get_data()

active_assets=list(prod_info.customer_asset_identifier[(prod_info.active_status == 'Active')])

inactive_assets=list(prod_info.customer_asset_identifier[(prod_info.active_status =='Inactive')])

for i in range(len(products)):
        if prod_list[i][0] in notes_prod:
                 prod_list[i][1]+=1
        if prod_list[i][0] in valid_list:
                prod_list[i][2]+=1
        if prod_list[i][0] in error_log_list:
                prod_list[i][3]+=1
        if prod_list[i][0] in install_date_error_list:
                prod_list[i][4]+=1
        if prod_list[i][0] in notes_60:
                prod_list[i][5]+=1
        if prod_list[i][0] in no60_notes:
                prod_list[i][6]+=1
        if prod_list[i][0] in no_notes_list:
                prod_list[i][7]+=1
        if prod_list[i][0] in perfect_notes:
                prod_list[i][8]+=1
        if prod_list[i][0] in daily_alone_list:
                prod_list[i][9]+=1
        if prod_list[i][0] in daily_list:
                prod_list[i][10]+=1
        if prod_list[i][0] in man_list:
                prod_list[i][11]+=1
        if prod_list[i][0] in active_assets:
               prod_list[i][12]+=1
        if prod_list[i][0] in inactive_assets:
                prod_list[i][13]+=1
         
print prod_list
df=panda.DataFrame(prod_list)
df.columns=['product_id','daily_notes_present','selected_for_ML','error_log','install_date_in_future','daily_work_for_60days','no_daily_work_for_60_days','work_missing_for_15_consecutive_days','perfect_daily_work','Log status 1','Log 1&2','Log 2','Active Asset','inactive Asset']
             
df=df.append(df.sum(axis=0),ignore_index=True)
df.to_csv('data_analysis.csv',index=False)
~                                                                                                                                                                                                                         
~                                                                      
