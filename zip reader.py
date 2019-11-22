# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:03:57 2019

@author: Eric
"""
import os
import io
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine

# setup file location
path = r'C:\Code projects\opioids'
file = 'zips'
full_path = os.path.join(path, file + '.csv')

# read rows set number of rows
zip_df = pd.read_csv(full_path)

# remove all columns except zip and 2010 population
zip_df.drop(zip_df.columns[[1,2,3,4,5,6,8]], axis = 1, inplace = True)

# pad 0's on zipcodes that start with 0 or 00
zip_df['zip_code'] = zip_df['zip_code'].apply(lambda x: '{0:0>5}'.format(x))

# rename columns
zip_df.rename(columns={'zip_code':'zip',
                       'y-2010':'2010'}, inplace=True)

# Start SQL
# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

# Opens a cursor for the table create
table_cur = conn.cursor()

# sql statement to create a table for each state
create_table = sql.SQL('''
                       CREATE TABLE zip (
                       zip INT,
                       population INT
                       )''')

# executes the query
table_cur.execute(create_table)

# closes the SQL cursor, commits all SQL transactions
table_cur.close()
conn.commit()

# truncates the table
zip_df.head(0).to_sql('zip', engine, if_exists='replace',index=False)

# opens a cursor for the insert
insert_cur = conn.cursor()

# sets up an io string object
output = io.StringIO()

# converts the dataframe to a csv with the destination being the io object
zip_df.to_csv(output, sep='\t', header=False, index=False)

# goes to index 0 of the io object
output.seek(0)

# gets the value
contents = output.getvalue()

# converts null values to '', postgre doesnt accept null
insert_cur.copy_from(output, 'zip', null="")

# closes the insert cursor
insert_cur.close()

# commits the insert
conn.commit()

# closes connection to the SQL server
conn.close()