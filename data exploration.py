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