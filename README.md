# testModel
functions test ground for the new ARCA model 

      1: Configure UI to get error_code_mapping from customer
      2: Gather data from ekryp_schema into ML_reference schema
      3: filter assets using reasons_mapping [DONE >>> final_asset_report.py]
      4: send report [DONE >>> sendmail.py]
      5: build feature table
          5.1: get filtered product_list [DONE]
          5.2: settle on feature_set
          5.3: fill up missig data
          5.4: map firmware history to products
          5.5: find pm_dates and map notes/cumulative notes for those dates
          5.6: set daily_work timeframe, eg: 30 days or 1 week etc
          5.7: calculate days_since_failure
          5.8: calculate number of incidents from service_request_record
          5.9: error_code mapping with code_id and criticality
          5.10: assign model info from asset_feature table
          5.11: create feature_set.csv file
      6: ML model
          6.1: settle on prediction window/ prediction frequency
          6.2: split train and test data
          6.3: train model
          6.4: settle on what results/ output should be
          6.5: generate ROCs, Precision graphs
          6.6: evaluate test model
          6.7: send results
      7: create python file to send report/output files to emailCreds
      8: form ml_model.sh pipeline
