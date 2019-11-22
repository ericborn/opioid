# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 09:28:22 2019

@author: Eric
"""
import psycopg2
import pandas as pd
from sys import exit
from psycopg2 import sql
from sqlalchemy import create_engine

# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
#conn = engine.raw_connection()

# tables are named after the US states
table_name = 'alaska'

# CAUSES COMPUTER TO RUN OOM
# select entire table to datafrmae
#state_df = pd.read_sql_table(table_name, engine)

# Select with limit and table name as variable
state_df = pd.read_sql_query(sql.SQL('''SELECT * FROM {} LIMIT 100''').format(sql.Identifier(
                       table_name)), engine)
