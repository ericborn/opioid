# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 11:10:12 2019

@author: Eric
Counts
opioids - 178948026 records - 1 min 42 sec

"""
import psycopg2
from sys import exit
from psycopg2 import sql
from sqlalchemy import create_engine

# full state list
states = ['Alabama','Arizona','Arkansas','California','Colorado',
          'Connecticut','Washington_dc','Delaware','Florida','Georgia',
          'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky',
          'Louisiana','Maine','Maryland','Massachusetts','Michigan',
          'Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada',
          'New_Hampshire','New_Jersey','New_Mexico','New_York',
          'North_Carolina','North_Dakota','Ohio','Oklahoma','Oregon',
          'Pennsylvania','Rhode_Island','South_Carolina','South_Dakota',
          'Tennessee','Texas','Utah','Vermont','Virginia','Washington',
          'West_Virginia','Wisconsin','Wyoming']

# convert states to lowercase
states = [x.lower() for x in states]


# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

i = 0

counts = []

# query to select the count from the table
#select_query = sql.SQL("SELECT COUNT (*) FROM arizona")

for state in states: 
    # Opens a cursor for the table create
    count_cursor = conn.cursor()
    
    table_name = states[i]

    # query to select the count from the table
    select_query = sql.SQL("SELECT COUNT (*) FROM {}").format(
            sql.Identifier(table_name))
    
    # executes the query
    count_cursor.execute(select_query)
    
    # appends the result to the counts list
    counts.append(count_cursor.fetchall()[0][0]) 
    
# closes the SQL cursor, commits all SQL transactions
count_cursor.close()

    