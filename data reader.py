# -*- coding: utf-8 -*-
"""
Eric Born

File to read the arcos tsv files and move the data into postgre db
"""
import os
import csv
import pandas as pd
from sys import exit


path = r'F:\opioid data'
file = 'arcos_all_washpost'
full_path = os.path.join(path, file + '.tsv')

# read 10 rows
arcos_df = pd.read_csv(full_path,nrows=500, sep='\t') 

# output 
arcos_df.to_csv(path+'\\test.csv')
   
#with open(full_path) as tsvfile:
#  reader = csv.DictReader(tsvfile, dialect='excel-tab')
#  for row in reader:
#      print(row)
#        
## read csv file into dataframe
#try:
#    lol_df = pd.read_csv(ticker_file)
#    print('opened file for ticker: ', data,'\n')
#
#except Exception as e:
#    print(e)
#    exit('failed to read LoL data from: '+ str(data)+'.csv')