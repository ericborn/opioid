# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 10:08:24 2019

@author: Eric
"""
#from psycopg2 import connect, DatabaseError

import psycopg2
from sys import exit
from psycopg2 import sql
#from psycopg2.extensions import quote_ident
from sqlalchemy import create_engine

#####
# state
#####
    
#states_test = ['Alabama','Alaska','Arizona']
#initials_test = ['AL','AK','AZ']

states = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado',
          'Connecticut','Washington_dc','Delaware','Florida','Georgia',
          'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky',
          'Louisiana','Maine','Maryland','Massachusetts','Michigan',
          'Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada',
          'New_Hampshire','New_Jersey','New_Mexico','New_York',
          'North_Carolina','North_Dakota','Ohio','Oklahoma','Oregon',
          'Pennsylvania','Rhode_Island','South_Carolina','South_Dakota',
          'Tennessee','Texas','Utah','Vermont','Virginia','Washington',
          'West_Virginia','Wisconsin','Wyoming']

# convert to lowercase
states = [x.lower() for x in states]

initials = ['AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID',
            'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
            'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
            'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

###
# Start SQL
###

# Creates a connection string
# SQL Alchemy
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

# index variable
i = 0

for state in states: 
    # Opens a cursor to write the data
    cur = conn.cursor()
    
    table_name = states[i]
    init_name = initials[i]

    # used to remove data from all state tables 
    # without deleting the tables themselves
    #query = sql.SQL('truncate {}').format(sql.Identifier(table_name))

    # stores the current state and state initial into the query
    # query inserts into state name table with * selected from opioids table
    # where buyer_state is equal to the states initials
    # formats the table name to an identifier and 
    # the initials to a string literal
    query = sql.SQL("INSERT INTO {0} SELECT * FROM opioids WHERE buyer_state = {1}").format(
            sql.Identifier(table_name), sql.Literal(init_name))
    
    # print the query as it would be passed to SQL
    #print(query.as_string(cur))
    
    # executes the query
    cur.execute(query)
    
    # iterate to the next index in the states
    i += 1

    # closes the SQL cursor, commits all SQL transactions
    cur.close()
    conn.commit()

# closes connection to the SQL server
conn.close()

####
# End SQL
####