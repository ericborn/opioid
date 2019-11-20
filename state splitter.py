# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 10:08:24 2019

@author: Eric Born
The purpose of this script is to segment the data from the opioids table into
separate tables for each state based on the buyer_state column.

The script will create a table for each state that is present in the data and
then insert the data for that state from the main table.

There was no data present from Alaska so the state was ignored.
There was data present for US territories, but these were not included
due to being only about 500 rows.
"""
import psycopg2
from sys import exit
from psycopg2 import sql
from sqlalchemy import create_engine

#####
# list setup
#####
 
# variables for testing   
#states_test = ['alabama','arizona']
#initials_test = ['AL','AZ']

# state list
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

# state initials list
initials = ['AL','AZ','AR','CA','CO','CT','DC','DE','FL','GA','HI','ID',
            'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO',
            'MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA',
            'RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

###
# Start SQL
###
# Creates a connection string
engine = create_engine('postgresql+psycopg2://python:password@localhost/arcos')

# raw connection
conn = engine.raw_connection()

# index variable
i = 0

for state in states: 
    # Opens a cursor for the table create
    table_cur = conn.cursor()
    
    # Opens a cursor to insert the data
    insert_cur = conn.cursor()
    
    table_name = states[i]
    init_name = initials[i]

    # used to remove data from all state tables 
    # without deleting the tables themselves
    #query = sql.SQL('truncate {}').format(sql.Identifier(table_name))
    
    # sql statement to create a table for each state
    create_table = sql.SQL('''
                           CREATE TABLE {} (
                           REPORTER_NAME TEXT,
                           REPORTER_ADDL_CO_INFO TEXT,
                           REPORTER_ADDRESS1 TEXT,
                           REPORTER_ADDRESS2 TEXT,
                           REPORTER_CITY TEXT,
                           REPORTER_STATE TEXT,
                           REPORTER_ZIP INTEGER,
                           REPORTER_COUNTY TEXT,
                           BUYER_BUS_ACT TEXT,
                           BUYER_NAME TEXT,
                           BUYER_ADDRESS1 TEXT,
                           BUYER_ADDRESS2 TEXT,
                           BUYER_CITY TEXT,
                           BUYER_STATE TEXT,
                           BUYER_ZIP INTEGER,
                           BUYER_COUNTY TEXT,
                           DRUG_NAME TEXT,
                           QUANTITY REAL,
                           UNIT TEXT,
                           TRANSACTION_DATE DATE,
                           CALC_BASE_WT_IN_GM DECIMAL,
                           DOSAGE_UNIT REAL,
                           Product_Name TEXT,
                           Combined_Labeler_Name TEXT,
                           Revised_Company_Name TEXT,
                           Reporter_family TEXT,
                           dos_str DECIMAL
                           )''').format(sql.Identifier(table_name))

    # executes the query
    table_cur.execute(create_table)
    
    # closes the SQL cursor, commits all SQL transactions
    table_cur.close()
    conn.commit()
    
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
    insert_cur.execute(query)
    
    # closes the SQL cursor, commits all SQL transactions
    insert_cur.close()
    conn.commit()
    
    # iterate to the next index in the states
    i += 1

# closes connection to the SQL server
conn.close()

####
# End SQL
####