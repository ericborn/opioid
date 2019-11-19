# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 10:08:24 2019

@author: Eric
"""
#from psycopg2 import connect, DatabaseError
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from sys import exit
from sqlalchemy import create_engine, MetaData, insert, Table, Column, String,\
                       Integer, Float, Boolean, VARCHAR, SmallInteger

states = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado',
          'Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho',
          'Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana',
          'Maine','Maryland','Massachusetts','Michigan','Minnesota',
          'Mississippi','Missouri','Montana','Nebraska','Nevada',
          'New_Hampshire','New_Jersey','New_Mexico','New_York',
          'North_Carolina','North_Dakota','Ohio','Oklahoma','Oregon',
          'Pennsylvania','Rhode_Island','South_Carolina','South_Dakota',
          'Tennessee','Texas','Utah','Vermont','Virginia','Washington',
          'West_Virginia','Wisconsin','Wyoming']

initials = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL',
           'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT',
           'NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
           'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

for st in states: 
    i = 0
    
    INSERT INTO st
    SELECT *
    FROM opioids
    WHERE state = initials[i]
    
    i += 1

#####
# 3 state test
#####
    
states_test = ['Alabama','Alaska','Arizona']
initials_test = ['AL','AK','AZ']

i = 0

###
# Start SQL
###

# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

# Opens a cursor to write the data
cur = conn.cursor()

for st in states_test: 
    print(states_test[i], initials_test[i])
    i += 1
    
cur.executemany(
    '''
    INSERT INTO ?
    SELECT *
    FROM opioids
    WHERE state = ?
    ''', (states_test, initials_test)
)
cur.close()
conn.commit()
conn.close()

INSERT INTO st
SELECT *
FROM opioids
WHERE state = initials_test[i]
    
    
    
try:
    #Connects and creates the desired database
    conn = connect('dbname=arcos user=python password=password')

    #conn.setAutoCommit(true)
    cur = conn.cursor()
    #cur.execute('SET AUTOCOMMIT = ON')
    cur.executemany(
        '''
        INSERT INTO ?
        SELECT *
        FROM opioids
        WHERE state = ?
        ''', (states_test, initials_test)
    )
    cur.close()
    conn.commit()
    conn.close()
except (Exception, DatabaseError) as dbError:
    print(dbError)


####
# End SQL
####