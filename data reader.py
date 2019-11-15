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

# read rows
arcos_df = pd.read_csv(full_path,nrows=500, sep='\t')

# view transaction data column
#arcos_df.iloc[:,30]

# view row 89, all columns
#arcos_df.iloc[89,:]

# view by column name
#arcos_df['QUANTITY']

# converts int format to month/day/year
arcos_df['TRANSACTION_DATE'] = pd.to_datetime(arcos_df['TRANSACTION_DATE'], format='%m%d%Y')

# convert QUANTITY from float to int
arcos_df['QUANTITY'] = arcos_df['QUANTITY'].astype('int64')

# output 
arcos_df.to_csv(path+'\\test.csv', index = False)

   
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