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
path = r'F:\opioid data'
file = 'arcos_all_washpost'
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
    chunksize = 150000
    i = 0
    j = 1
    
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
#        arcos_df['BUYER_ADDRESS2'].apply(lambda x: '601 J ST' 
#                if x == r'\601 \"\"J\"\" ST\""' else None)
        
        # selects all columns that are of type object
        str_df = arcos_df.select_dtypes([np.object])

        # stacks all rows, decodes to utf-8 then unstacks
        str_df = str_df.stack().str.decode('utf-8').unstack()
        
        # replaces the old data with the new utf-8 data
        for col in str_df:
            arcos_df[col] = str_df[col]
        
        # fill na with blank string
        arcos_df = arcos_df.fillna(value = '')
        
        # increment index
        arcos_df.index += j
        i+=1
     
        ###   
        # SQL
        ###
        meta = MetaData()
    
        # Creates a connection string
        engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

        # raw connection
        conn = engine.raw_connection()

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
        contents = output.getvalue()

        # Copys from the stream to the opioids table
        cur.copy_from(output, 'opioids', null="") # null values become ''

        # Commits on the connection to the database
        conn.commit()

        # Added but not tested, 
        # closing the connection after each insert
        # should probably be opening a connection outside of the loop
        # and closing once the loop completes
        conn.close()

        j = arcos_df.index[-1] + 1

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

# read rows set number of rows
arcos_df = pd.read_csv(full_path,nrows=15000, sep='\t')

# view transaction data column
#arcos_df.iloc[:,30]

# view row 89, all columns
#arcos_df.iloc[1,:]

# view by column name
#arcos_df['QUANTITY']

# drops unneeded columns
arcos_df.drop(arcos_df.columns[[0, 1, 10, 13, 20, 21, 22, 26, 27, 28, 29, 33, 
                                35, 36, 37]], axis = 1, inplace = True)

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


###
# Start SQL
###
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

####
# End SQL
####

######
# End fixed size read and insert into db
######

####
# Start batch testing
# https://stackoverflow.com/questions/42900757/sequentially-read-huge-csv-file-in-python
####

# read rows set number of rows
test_df = pd.read_csv(full_path,nrows=106, sep='\t')

# converts int format to month/day/year
test_df['TRANSACTION_DATE'] = pd.to_datetime(test_df['TRANSACTION_DATE'], 
                                              format='%m%d%Y')

# convert QUANTITY from float to int
test_df['QUANTITY'] = test_df['QUANTITY'].astype('int64')
test_df['DOSAGE_UNIT'] = test_df['DOSAGE_UNIT'].astype('int64')
test_df['MME_Conversion_Factor'] = test_df['MME_Conversion_Factor'].astype(
                                                                      'int64')
# save to csv 
test_df.to_csv(path+'\\test.csv', index = False)

# setup file location
test_file = 'test'
test_full_path = os.path.join(path, test_file + '.csv')

# setup variables
chunksize = 10
i = 0
j = 1

# for loop to read csv and write to db
for df in pd.read_csv(test_full_path, chunksize=chunksize, iterator=True):
    #test_df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
    test_df = df
    df.index += j
    i+=1
    
    meta = MetaData()

    # Creates a connection string
    engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')
    
    # Creates a table using the column names and datatypes defined in the dataframe
    test_df.head(0).to_sql('take', engine, if_exists = 'replace', \
                            index = False)
    
##SQL    
    # raw connection
    conn = engine.raw_connection()
    
    # Opens a cursor to write the data
    cur = conn.cursor()
    
    # prepares an in memory IO stream
    output = io.StringIO()
    
    # converts the dataframe contents to csv format and the IO steam as its destination
    test_df.to_csv(output, sep='\t', header=False, index=False)
    
    # sets the file offset position to 0
    output.seek(0)
    
    # retrieves the contents of the output stream
    contents = output.getvalue()
    
    # Copys from the stream to the opioids table
    cur.copy_from(output, 'opioids', null="") # null values become ''
    
    # Commits on the connection to the database
    conn.commit()
##SQL
    j = df.index[-1] + 1

####
# End batch testing
####