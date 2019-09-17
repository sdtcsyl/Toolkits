# -*- coding: utf-8 -*-
"""
Created on May 15 2019
"""

import os
import pandas as pd
import zipfile
from datetime import date
from os import listdir
from os.path import isfile, join
#import sys

project = 'freddiemac' ##please amend the project name

today = date.today()


filepath = os.getcwd()
#return the 'scripts' folder path
parpath = filepath #os.path.dirname(filepath)
#return the parent folder of the 'scripts' folder



def createfolder(folder_path):
    directory = folder_path
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


db_path = createfolder(parpath + '\\Data\\DB') + '\\'
rawdata_path = createfolder(parpath + '\\Data\\rawdata') + '\\'
files_path = parpath+'\\' #createfolder(parpath + '\\Files') + '\\'
js_path = createfolder(parpath + '\\Data\\Json') + '\\'
public_log_path = createfolder('C:\\Users\\Public\\Documents\\'+ project +'\\Logging') + '\\logging.txt'
log_path = createfolder(parpath + '\\logging') + '\\' + today.strftime("%Y_%m_%d") + '.txt'
unzip_path = createfolder(parpath + '\\Data\\unzipped_file')+'\\'
html_path = createfolder(parpath + '\\Data\\html') + '\\'

def writelog(txt):
    txt_writer=open(public_log_path,'a')  #append
    txt_writer.writelines(txt)
    txt_writer.close()
    
    txt_writer=open(log_path,'a')  #append
    txt_writer.writelines(txt)
    txt_writer.close()
    

def read_txt_2_df(file_name, seperator='|', header=None, rows=200000):
    ##Read bulk data
    ##generator
    data = pd.read_csv(file_name, sep=seperator, header=header, dtype=str, iterator=True)    
    yield data.get_chunk(rows)

def read_txt_2_line(file_name):
    fh = open(file_name)
    for line in fh:
        print(line)
    fh.close()


def unzip(path_to_zipped_file):
    if zipfile.is_zipfile(path_to_zipped_file):
        with zipfile.ZipFile(path_to_zipped_file, 'r') as zip_ref:
            zip_ref.extractall(path=unzip_path)
        
def list_files(mypath):
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    return files

def unzip_rawdata():
    files = list_files(rawdata_path)
    for file in files:
        unzip(rawdata_path+file)


     