# -*- coding: utf-8 -*-
"""
Created on May 15 2019
"""

import sqlite3
import files as Files

class database:
    def __init__(self, db):
        self.db = db
        self.tables = []
        self.conn = sqlite3.connect(Files.db_path + self.db + '.db')
        self.conn.close() 
        
    def get_table(self, name):        
        return table(self.db, name)        

'''///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////'''

class table:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.fields=[]
        self.types = []
        self.primary_key = []
        self.foreign_key = ''
    
    def string_fields(self):
        return ','.join(self.fields)
    
    def string_fields_types(self):
        return ','.join([field + ' ' + fieldtype + ' primary key'   if field in self.primary_key else field + ' ' + fieldtype for field, fieldtype in zip(self.fields, self.types)])
    
    def string_qmarks(self):
        return ','.join(['?']*len(self.fields))
    
    def add_foreign_key(self, flds, tbs, tbs_flds):
        '''please use it before create_table'''
        self.foreign_key = ','.join([' FOREIGN KEY (' + fld + ') REFERENCES table_' + tb + ' (' + tbs_fld + ')' for fld, tb, tbs_fld in zip(flds, tbs, tbs_flds)])
        return self.foreign_key
   
    def sql_create_table(self):
        sql = 'create table if not exists table_' + self.name + ' (' +  self.string_fields_types() + self.foreign_key +  ')'
        return sql
     
    def sql_insert_table(self):
        sql = 'insert into table_' + self.name + ' (' +  self.string_fields() + ')values (' + self.string_qmarks() +')'
        return sql
    
    def sql_drop_table(self):
        sql = 'DROP table if exists table_' + self.name
        return sql
    
    def exe_sql(self, sql, **kwargs):
        conn = sqlite3.connect(Files.db_path + self.db + '.db')
        cursor = conn.cursor()
        if len(kwargs) == 0:
            cursor.execute(sql)
        else:
            if 'data' in kwargs:
                try:
                    cursor.execute(sql, (kwargs['data']))   #insert data
                except sqlite3.IntegrityError:
                    pass
        cursor.close()
        conn.commit()
        conn.close() 

    def exe_sql_w_return(self, sql, **kwargs):
        conn = sqlite3.connect(Files.db_path + self.db + '.db')
        cursor = conn.cursor()
        if len(kwargs) == 0:
            cursor.execute(sql)
        else:
            if 'data' in kwargs:
                try:
                    cursor.execute(sql, (kwargs['data']))   #insert data
                except sqlite3.IntegrityError:
                    pass
        res =cursor.fetchall()
        cursor.close()
        conn.commit()
        conn.close() 
        return res

    def func_write_table(self, data):        
        self.exe_sql(self.sql_create_table())
        self.exe_sql(self.sql_insert_table(), data=data)
        
    def func_count_by_and(self, vares, var_operators, var_values): 
        condition = ' and '.join([var+' '+ var_operator +" '"+  var_value+ "' " for var, var_operator, var_value in zip(vares, var_operators, var_values)  ])
        sql = 'select count(*) from table_' + self.name + ' where ' +  condition
        counts = self.exe_sql_w_return(sql)
        return counts[0][0]
    
    def func_count_by_or(self, vares, var_operators, var_values): 
        condition = ' or '.join([var+' '+ var_operator +" '"+  var_value+ "' " for var, var_operator, var_value in zip(vares, var_operators, var_values)  ])
        sql = 'select count(*) from table_' + self.name + ' where ' +  condition
        counts = self.exe_sql_w_return(sql)
        return counts[0][0]
    
    #def func_check_w_table(self, other_table):
    
    def func_import_from_df(self, df):
        ## save the dataframe into database
        conn = sqlite3.connect(Files.db_path + self.db + '.db')
        try:
            df.to_sql(name='table_' + self.name, con=conn, if_exists = 'append')
        except sqlite3.IntegrityError:
            pass
        conn.commit()
        conn.close() 
        
    
'''///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////
   ///////////////////////////////////////////////////////////////////'''

class table_origination(table):
    ###ORIGINATION DATA FILE
    def __init__(self, db):
        super(table_origination,self).__init__(db,'origination')
        self.fields = ['CREDIT_SCORE',   ##CREDIT SCORE 
                     'FIRST_PAYMENT_DATE', ##FIRST PAYMENT DATE 
                     'FIRST_TIME_HOMEBUYER_FLAG', ##FIRST TIME HOMEBUYER FLAG 
                     'MATURITY_DATE', ##MATURITY DATE  
                     'METROPOLITAN_DIVISION', ##METROPOLITAN STATISTICAL AREA (MSA) OR METROPOLITAN DIVISION  
                     'MORTGAGE_INSURANCE_PERCENTAGE', ##MORTGAGE INSURANCE PERCENTAGE 
                     'NUMBER_OF_UNITS', ##NUMBER OF UNITS
                     'OCCUPANCY_STATUS', ##OCCUPANCY STATUS
                     'CLTV', ##ORIGINAL COMBINED LOAN-TO-VALUE (CLTV) 
                     'OGTIR', ##ORIGINAL DEBT-TO-INCOME (DTI) RATIO 
                     'ORIGINAL_UPB', ##ORIGINAL UPB 
                     'ORIGINAL_LTV', ##ORIGINAL LOAN-TO-VALUE (LTV)  
                     'ORIGINAL_INTEREST_RATE', ##ORIGINAL INTEREST RATE 
                     'CHANNEL', ##CHANNEL 
                     'PPM_FLAG', ##PREPAYMENT PENALTY MORTGAGE (PPM) FLAG 
                     'PRODUCT_TYPE', ##PRODUCT TYPE  
                     'PROPERTY_STATE', ##PROPERTY STATE  
                     'PROPERTY_TYPE', ##PROPERTY TYPE
                     'POSTAL_CODE', ##POSTAL CODE  
                     'LOAN_SEQUENCE_NUMBER', ##LOAN SEQUENCE NUMBER  
                     'LOAN_PURPOSE', ##LOAN PURPOSE  
                     'ORIGINAL_LOAN_TERM', ##ORIGINAL LOAN TERM  
                     'NUMBER_OF_BORROWERS', ##NUMBER OF BORROWERS 
                     'SELLER_NAME', ##SELLER NAME  
                     'SERVICER_NAME', ##SERVICER NAME 
                     'SUPER_CONFORMING_FLAG', ##SUPER CONFORMING FLAG 
                     'Pre_HARP_LOAN_SEQUENCE_NUMBER' ##Pre-HARP LOAN SEQUENCE NUMBER 
                     ]   
        self.types = ['varchar(2)']*len(self.fields)
        self.primary_key = ['LOAN_SEQUENCE_NUMBER']
        self.exe_sql(self.sql_create_table())

    def func_count_by_sf(self, sf): #按照省份来数个数
        counts = self.func_count_by_and('sf','=',sf)
        #sql = 'select count(*) from table_' + self.name + ' where sf = ' +  "'" + (sf) + "'"
        #counts = self.exe_sql_w_return(sql)
        return counts[0][0]
    
    def func_select_swsbm_web(self):
        sql = 'select swsbm, web from table_' + self.name
        counts = self.exe_sql_w_return(sql)
        return counts
    
    def func_update(self, swsbm, fld, fld_val):
        sql = 'UPDATE table_' + self.name + ' SET ' + fld +' = ' + "'" + str(fld_val) + "'"  + ' WHERE swsbm = ' +  "'" + str(swsbm) + "'" 
        self.exe_sql(sql)

    def func_check_w_basic(self):
        sql = 'select swsbm, web from table_main where swsbm not in (select gsbm from table_basic)'
        return self.exe_sql_w_return(sql)
        
class table_monthly(table):
    ###MONTHLY PERFORMANCE DATA FILE
    def __init__(self, db):
        super(table_monthly,self).__init__(db,'monthly')
        self.fields = ['Id',
                     'LOAN_SEQUENCE_NUMBER',   ##LOAN SEQUENCE NUMBER  
                     'MONTHLY_REPORTING_PERIOD', ##MONTHLY REPORTING PERIOD 
                     'CURRENT_ACTUAL_UPB', ##CURRENT ACTUAL UPB
                     'CURRENT_LOAN_DELINQUENCY_STATUS', ##CURRENT LOAN DELINQUENCY STATUS   
                     'LOAN_AGE', ##LOAN AGE  
                     'REMAINING_MONTHS_TO_LEGAL_MATURITY', ##REMAINING MONTHS TO LEGAL MATURITY 
                     'REPURCHASE_FLAG', ##REPURCHASE FLAG
                     'MODIFICATION_FLAG', ##MODIFICATION FLAG 
                     'ZERO_BALANCE_CODE', ##OZERO BALANCE CODE 
                     'ZERO_BALANCE_EFFECTIVE_DATE', ##ZERO BALANCE EFFECTIVE DATE
                     'CURRENT_INTEREST_RATE', ##CURRENT INTEREST RATE 
                     'CURRENT_DEFERRED_UPB', ##CURRENT DEFERRED UPB  
                     'DDLPI', ##DUE DATE OF LAST PAID INSTALLMENT (DDLPI) 
                     'MI_RECOVERIES', ##MI RECOVERIES 
                     'NET_SALES_PROCEEDS', ##NET SALES PROCEEDS 
                     'NON_MI_RECOVERIES', ##NON MI RECOVERIES  
                     'EXPENSES', ##EXPENSES  
                     'LEGAL_COSTS', ##LEGAL COSTS
                     'MAINTENANCE_AND_PRESERVATION_COSTS', ##`MAINTENANCE AND PRESERVATION COSTS  
                     'TAXES_AND_INSURANCE', ##TAXES AND INSURANCE  
                     'MISCELLANEOUS_EXPENSES', ##MISCELLANEOUS EXPENSES  
                     'ACTUAL_LOSS_CALCULATION', ##ACTUAL LOSS CALCULATION  
                     'MODIFICATION_COST', ##MODIFICATION COST 
                     'STEP_MODIFICATION_FLAG', ##STEP MODIFICATION FLAG  
                     'DEFERRED_PAYMENT_MODIFICATION', ##DEFERRED PAYMENT MODIFICATION  
                     'ESTIMATED_LOAN_TO_VALUE' ##ESTIMATED LOAN TO VALUE (ELTV)
                     ]   
        self.types = ['varchar(2)']*len(self.fields)
        self.primary_key = ['Id']
        self.exe_sql(self.sql_create_table())

        