# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 13:52:24 2019

@author: Eric
"""
import os
import io
import csv
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from sys import exit
from sqlalchemy import create_engine

# list of states to explore
states = ['tennessee','west_virginia','alabama',
          'california','hawaii','washington_dc']

# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

sellers = []
  
i = 0   
for state in states: 
    # Opens a cursor for the table create
    select_cursor = conn.cursor()
    
    #table_name = states[i]

    # query to select the count from the table
    select_query = sql.SQL('''SELECT DISTINCT reporter_name, COUNT(*) AS count
                              FROM {}
                              GROUP BY reporter_name
                              ORDER BY count DESC
                              LIMIT 10''').format(
            sql.Identifier(states[0])) 
            #sql.Identifier(states[i]))
    
    # executes the query
    select_cursor.execute(select_query)
    
    # appends query results to sellers list
    sellers.append(select_cursor.fetchall())
    
    i+=1
    
# closes the SQL cursor, commits all SQL transactions
select_cursor.close()

# flattens sellers into a single list of tuples
flat = [item for sublist in sellers for item in sublist]

# empty list
data = []

# creates a new list with state name, company and total
for i in range(len(flat)):
    if i < 9:
        data.append([states[0], flat[i][0], flat[i][1]])
    if i > 9 and i < 19:
        data.append([states[1], flat[i][0], flat[i][1]])
    if i > 19 and i < 29:
        data.append([states[2], flat[i][0], flat[i][1]])
    if i > 29 and i < 39:
        data.append([states[3], flat[i][0], flat[i][1]])
    if i > 39 and i < 49:
        data.append([states[4], flat[i][0], flat[i][1]])     
    if i > 49:
        data.append([states[5], flat[i][0], flat[i][1]])          
        
        
# turns the data list into a dataframe
state_df = pd.DataFrame(data, columns = ['state', 'company', 'total'])
