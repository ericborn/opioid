# -*- coding: utf-8 -*-
"""
Eric Born

File to read the arcos tsv files and move the data into postgre db
"""
import os
import io
import csv
import pandas as pd
import numpy as np
import psycopg2
from sys import exit
from sqlalchemy import create_engine, MetaData, insert, Table, Column, String,\
                       Integer, Float, Boolean, VARCHAR, SmallInteger

# setup file location
path = r'E:\opioid data'
file = 'arcos_all'
full_path = os.path.join(path, file + '.tsv')

####
# End setup
####

####
# start batch read/insert
####

# view transaction data column
#arcos_df.iloc[:,30]

# view row 89, all columns
#arcos_df.iloc[0,:]

# view by column name
#arcos_df['QUANTITY']

#fake_data = [[92159], ["PINELLAS"]]
#
#fakedf = pd.DataFrame(fake_data, columns = ['BUYER_ZIP'])
#
## remove non-digits from BUYER_ZIP
#fakedf['BUYER_ZIP'].replace(to_replace=r'\D', value = '0',
#                                          regex = True, inplace = True)

# 40,750,000

def tsv_todb():
    # setup variables
    chunksize = 500000
    i = 0
    j = 1
    
    # Creates a connection string
    engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

    # raw connection
    conn = engine.raw_connection()
    
    # for loop to read csv and write to db
    for arcos_df in pd.read_csv(full_path, chunksize=chunksize, sep='\t',
                          iterator=True, encoding='utf-8'):
        
        # drop unneeded columns
        arcos_df.drop(arcos_df.columns[[0, 1, 10, 13, 20, 21, 22, 26, 27, 28, 29, 
                                        33, 35, 36, 37]], axis = 1, inplace = True)
        
        # converts int format to month/day/year
        arcos_df['TRANSACTION_DATE'] = pd.to_datetime(arcos_df['TRANSACTION_DATE'], 
                                                      format='%m%d%Y')
        
        # remove / from REPORTER_ADDL_CO_INFO
        arcos_df['REPORTER_ADDL_CO_INFO'].replace(to_replace=r'/', value = ' ',
                                                  regex = True, inplace = True)
        
        arcos_df['BUYER_BUS_ACT'].replace(to_replace=r'/', value = ' ',
                                         regex = True, inplace = True)
        
        arcos_df['Product_Name'].replace(to_replace=r'/', value = ' ',
                                         regex = True, inplace = True)
        
        # removes \ and , from buyer_name
        arcos_df['BUYER_NAME'].replace(to_replace=r'\\', value = '',
                                           regex = True, inplace = True)
        arcos_df['BUYER_NAME'].replace(to_replace=r',', value = '',
                                           regex = True, inplace = True)
        
        # removes \ and " from buyer_address1
        arcos_df['BUYER_ADDRESS1'].replace(to_replace=r'\\', value = '',
                                           regex = True, inplace = True)
        arcos_df['BUYER_ADDRESS1'].replace(to_replace=r'"', value = '',
                                           regex = True, inplace = True)
        
        # removes \ and " from buyer_address2
        arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'\\', value = '',
                                           regex = True, inplace = True)
        arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'"', value = '',
                                           regex = True, inplace = True)
        
        # remove non-digits from REPORTER_ZIP
        arcos_df['REPORTER_ZIP'].replace(to_replace=r'\D', value = '0',
                                                  regex = True, inplace = True)
        
        # remove non-digits from BUYER_ZIP
        arcos_df['BUYER_ZIP'].replace(to_replace=r'\D', value = '0',
                                                  regex = True, inplace = True)
        
        # remove non-digits from QUANTITY
        arcos_df['QUANTITY'].replace(to_replace=r'\D', value = '0',
                                                  regex = True, inplace = True)
        
        # remove non-digits from DOSAGE_UNIT
        arcos_df['DOSAGE_UNIT'].replace(to_replace=r'\D', value = '0',
                                                  regex = True, inplace = True)
        
        # remove non-digits from dos_str
        arcos_df['dos_str'].replace(to_replace=r'\D', value = '0',
                                                  regex = True, inplace = True)
        
        # fill na with blank string
        arcos_df = arcos_df.fillna(value = '')
        
        # increment index
        arcos_df.index += j
        i+=1
     
        ###   
        # SQL
        ###
        
        #meta = MetaData()

        # Opens a cursor to write the data
        cur = conn.cursor()

        # prepares an in memory IO stream
        output = io.StringIO()

        # converts the dataframe contents to csv format and
        # the IO steam as its destination
        arcos_df.to_csv(output, sep='\t', header=False, index=False)

        # sets the file offset position to 0
        output.seek(0)

        # retrieves the contents of the output stream
        #contents = output.getvalue()

        # Copys from the stream to the opioids table
        # null values become ''
        cur.copy_from(output, 'opioids_full', null="")

        # Commits on the connection to the database
        conn.commit()

        # increments the iterator
        j = arcos_df.index[-1] + 1

    # closes the connection after the loop finishes
    conn.close()

    ####
    # End SQL
    ####

# Runs the function to read the data and insert into the database
tsv_todb()

######
# End batch read/insert
######

######
# Single fixed size read and insert into db
######

#with open(full_path) as fp:
#    reader=csv.reader(fp, delimiter='\t')    
#    rows=[row for idx, row in enumerate(reader) if idx == 28]
    
###TODO
# BUILD BATCH READER THAT READS AND MAKES A CHECK FOR THE LAST ROW NUMBER
# ONCE IT PASSES 16605635 BY A SMALL NUMBER IT SAVES THE CURRENG BATCH
# TO A DATAFRAME AND BREAKS THE LOOP

# read rows set number of rows
arcos_df = pd.read_csv(full_path,nrows=1, sep='\t')

# view transaction data column
#arcos_df.iloc[:,30]

# view row 89, all columns
#arcos_df.iloc[16500000,:]

# view by column name
#arcos_df['QUANTITY']

# drops unneeded columns
arcos_df.drop(arcos_df.columns[[0, 1, 10, 13, 20, 21, 22, 26, 27, 28, 29, 33, 
                                35, 36, 37]], axis = 1, inplace = True)

# converts int format to month/day/year
arcos_df['TRANSACTION_DATE'] = pd.to_datetime(arcos_df['TRANSACTION_DATE'], 
                                              format='%m%d%Y')

# remove / from REPORTER_ADDL_CO_INFO
arcos_df['REPORTER_ADDL_CO_INFO'].replace(to_replace=r'/', value = ' ',
                                          regex = True, inplace = True)

arcos_df['BUYER_BUS_ACT'].replace(to_replace=r'/', value = ' ',
                                 regex = True, inplace = True)

arcos_df['Product_Name'].replace(to_replace=r'/', value = ' ',
                                 regex = True, inplace = True)

# removes \ and " from buyer_address1
arcos_df['BUYER_ADDRESS1'].replace(to_replace=r'\\', value = '',
                                   regex = True, inplace = True)
arcos_df['BUYER_ADDRESS1'].replace(to_replace=r'"', value = '',
                                   regex = True, inplace = True)

# removes \ and " from buyer_address2
arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'\\', value = '',
                                   regex = True, inplace = True)
arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'"', value = '',
                                   regex = True, inplace = True)

# fill na with blank string
arcos_df = arcos_df.fillna(value = '')

# save to csv 
##arcos_df.to_csv(path+'\\test.csv', index = False)

####
## Start SQL
####

# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

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
cur.copy_from(output, 'opioids_full', null="") # null values become ''

# Commits on the connection to the database
conn.commit()

####
# End SQL
####