# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 08:56:26 2019

"""

import files as fl
import database as db

db_name = 'Freddiemac_Loan_Level_Dataset'

db_origination = db.table_origination(db_name)
db_monthly = db.table_monthly(db_name)

fl.unzip_rawdata()

files = fl.list_files(fl.unzip_path)
for file in files:
    if 'txt' in file:
        if 'historical_data1_Q' in file:
             data = fl.read_txt_2_df(fl.unzip_path+file, seperator='|', header=None, rows = 2000000)
             for datum in data:
                 datum.columns = db_origination.fields[0:26]
                 datum.set_index('LOAN_SEQUENCE_NUMBER', inplace=True)
                 db_origination.func_import_from_df(datum)
        if 'historical_data1_time_Q' in file:
            data = fl.read_txt_2_df(fl.unzip_path+file, seperator='|', header=None, rows = 2000000)
            for datum in data:
                datum.columns=db_monthly.fields[1:28]
                datum['Id'] = datum['LOAN_SEQUENCE_NUMBER'] +' '+ datum['MONTHLY_REPORTING_PERIOD']
                datum.set_index('Id', inplace=True)
                db_monthly.func_import_from_df(datum)
