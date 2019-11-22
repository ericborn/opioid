# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 14:03:57 2019

@author: Eric
"""
import os
import pandas as pd

# setup file location
path = r'C:\Code projects\opioids'
file = 'zips'
full_path = os.path.join(path, file + '.csv')

# read rows set number of rows
zip_df = pd.read_csv(full_path)

# remove columns gameId, creationTime, seasonId and winner
zip_df.drop(zip_df.columns[[1,2,3,4,5,6,8]], axis = 1, inplace = True)

# TODO
# pad 0's on zipcodes that start with 0 or 00

# need to write a SQL statement to create a table for zip
# columns: zip, population both int