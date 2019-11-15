# -*- coding: utf-8 -*-
"""
Eric Born

File to read the arcos tsv files and move the data into postgre db
"""

import csv

path = r'F:\opioid data'

file = 'arcos_all_washpost.tsv'

with open('myfile.tsv') as tsvfile:
  reader = csv.DictReader(tsvfile, dialect='excel-tab')
  for row in reader:
    print(row)