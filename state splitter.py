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
# 3 state test
#####
    
states_test = ['alabama','alaska','arizona']
initials_test = ['AL','AK','AZ']

###
# Start SQL
###

# Creates a connection string
# SQL Alchemy
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()


# Opens a cursor to write the data
cur = conn.cursor()

#query = sql.SQL("INSERT INTO {0} SELECT * FROM opioids WHERE buyer_state = {1}").format(
#            sql.Identifier(states_test[0]), sql.Literal(initials_test[0]))

#print(query.as_string(cur))

#cur.execute(query)

i = 0

for state in states_test: 
    table_name = states_test[i]
    init_name = initials_test[i]
    
    #print(i)
    # stores the current state and state initial into the query
    # query inserts into state name table with * selected from opioids table
    # where buyer_state is equal to the states initials
    # formats the table name to an identifier and 
    # the initials to a string literal
    query = sql.SQL("INSERT INTO {0} SELECT * FROM opioids WHERE buyer_state = {1}").format(
            sql.Identifier(table_name), sql.Literal(init_name))
    
    #print(query.as_string(cur))
    
    # executes the query
    cur.execute(query)
    i += 1

cur.close()
conn.commit()
conn.close()

####
# End SQL
####


#states = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado',
#          'Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho',
#          'Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana',
#          'Maine','Maryland','Massachusetts','Michigan','Minnesota',
#          'Mississippi','Missouri','Montana','Nebraska','Nevada',
#          'New_Hampshire','New_Jersey','New_Mexico','New_York',
#          'North_Carolina','North_Dakota','Ohio','Oklahoma','Oregon',
#          'Pennsylvania','Rhode_Island','South_Carolina','South_Dakota',
#          'Tennessee','Texas','Utah','Vermont','Virginia','Washington',
#          'West_Virginia','Wisconsin','Wyoming']
#
#initials = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL',
#           'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT',
#           'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
#           'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
#
#for st in states: 
#    i = 0
#    
#    INSERT INTO st
#    SELECT *
#    FROM opioids
#    WHERE state = initials[i]
#    
#    i += 1