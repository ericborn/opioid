# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 10:08:24 2019

@author: Eric
"""

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

states_test = ['Alabama','Alaska','Arizona']
initials_test = ['AL','AK','AZ']



for st in states_test: 
    i = 0
    
    INSERT INTO st
    SELECT *
    FROM opioids
    WHERE state = initials_test[i]
    
    i += 1