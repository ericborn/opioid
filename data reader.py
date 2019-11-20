# -*- coding: utf-8 -*-
"""
Eric Born

File to read the arcos tsv files and move the data into postgre db
"""
import os
import io
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
#arcos_df.iloc[89,:]

# view by column name
#arcos_df['QUANTITY']

def tsv_todb():
    # setup variables
    chunksize = 250000
    i = 0
    j = 1
    
    # Creates a connection string
    engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

    # raw connection
    conn = engine.raw_connection()
    
    # for loop to read csv and write to db
    for arcos_df in pd.read_csv(full_path, chunksize=chunksize, sep='\t',
                          iterator=True, encoding='utf-8'):
        
        arcos_df.drop(arcos_df.columns[[0, 1, 10, 13, 20, 21, 22, 26, 27, 28, 29, 
                                        33, 35, 36, 37]], axis = 1, inplace = True)
        
        try:
            # converts int format to month/day/year
            arcos_df['TRANSACTION_DATE'] = pd.to_datetime(
                                                    arcos_df['TRANSACTION_DATE'], 
                                                    format='%m%d%Y')
        except:
            pass
        
        # removes \ and " from buyer_address2
        arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'\\', value = '', 
                                           regex = True, inplace = True)    
        arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'"', value = '', 
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
        cur.copy_from(output, 'opioids_full', null="") # null values become ''

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

## read rows set number of rows
#arcos_df = pd.read_csv(full_path,nrows=1000000, sep='\t')
#
## view transaction data column
##arcos_df.iloc[:,30]
#
## view row 89, all columns
##arcos_df.iloc[801633,:]
#
## view by column name
##arcos_df['QUANTITY']
#
## drops unneeded columns
#arcos_df.drop(arcos_df.columns[[0, 1, 10, 13, 20, 21, 22, 26, 27, 28, 29, 33, 
#                                35, 36, 37]], axis = 1, inplace = True)
#
## converts int format to month/day/year
#arcos_df['TRANSACTION_DATE'] = pd.to_datetime(arcos_df['TRANSACTION_DATE'], 
#                                              format='%m%d%Y')
#
## removes \ and " from buyer_address2
#arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'\\', value = '', regex = True, inplace = True)    
#arcos_df['BUYER_ADDRESS2'].replace(to_replace=r'"', value = '', regex = True, inplace = True)    
#
## fill na with blank string
#arcos_df = arcos_df.fillna(value = '')
#
## save to csv 
#arcos_df.to_csv(path+'\\test.csv', index = False)
#
#####
### Start SQL
#####
#
## Creates a connection string
#engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')
#
## raw connection
#conn = engine.raw_connection()
#
## Opens a cursor to write the data
#cur = conn.cursor()
#
## prepares an in memory IO stream
#output = io.StringIO()
#
## converts the dataframe contents to csv format and the IO steam as its destination
#arcos_df.to_csv(output, sep='\t', header=False, index=False)
#
## sets the file offset position to 0
#output.seek(0)
#
## retrieves the contents of the output stream
#contents = output.getvalue()
#
## Copys from the stream to the opioids table
#cur.copy_from(output, 'opioids_full', null="") # null values become ''
#
## Commits on the connection to the database
#conn.commit()

####
# End SQL
####