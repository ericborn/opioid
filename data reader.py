# -*- coding: utf-8 -*-
"""
Eric Born

File to read the arcos tsv files and move the data into postgre db
"""
import os
import io
import csv
import pandas as pd
import psycopg2
from sys import exit
from sqlalchemy import create_engine, MetaData, insert, Table, Column, String,\
                       Integer, Float, Boolean, VARCHAR, SmallInteger

# setup file location
path = r'F:\opioid data'
file = 'arcos_all_washpost'
full_path = os.path.join(path, file + '.tsv')

# read rows set number of rows
#arcos_df = pd.read_csv(full_path,nrows=500, sep='\t')

# read rows to df
arcos_df = pd.read_csv(full_path, sep='\t')

# view transaction data column
#arcos_df.iloc[:,30]

# view row 89, all columns
#arcos_df.iloc[1,:]

# view by column name
#arcos_df['QUANTITY']

# converts int format to month/day/year
arcos_df['TRANSACTION_DATE'] = pd.to_datetime(arcos_df['TRANSACTION_DATE'], 
                                              format='%m%d%Y')

# convert QUANTITY from float to int
arcos_df['QUANTITY'] = arcos_df['QUANTITY'].astype('int64')
arcos_df['DOSAGE_UNIT'] = arcos_df['DOSAGE_UNIT'].astype('int64')
arcos_df['MME_Conversion_Factor'] = arcos_df['MME_Conversion_Factor'].astype(
                                                                      'int64')
# save to csv 
arcos_df.to_csv(path+'\\test.csv', index = False)


#######
# Start SQL
#######
meta = MetaData()

# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# Creates a table using the column names and datatypes defined in the dataframe
#arcos_df.head(0).to_sql('weaponproperties', engine, if_exists = 'replace', index = False)

# raw connection
conn = engine.raw_connection()

# Opens a cursor to write the data
cur = conn.cursor()

# prepares an in memory IO stream
output = io.StringIO()

# converts the dataframe contents to csv format and the IO steam as its destination
arcos_df.to_csv(output, sep='\t', header=False, index=False)

# sets the file offset position to 0
output.seek(0)

# retrieves the contents of the output stream
contents = output.getvalue()

# Copys from the stream to the opioids table
cur.copy_from(output, 'opioids', null="") # null values become ''

# Commits on the connection to the database
conn.commit()

#######
# End SQL
#######
   
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