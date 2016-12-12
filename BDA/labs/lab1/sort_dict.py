#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 10:31:18 2016

@author: ashraf
"""
temp_dict = dict()
#1st tuple is min and 2nd tuple for max
temp_dict[2000] = [(12345, 1.1), (12345, 2.13)]
temp_dict[2001] = [(12345, 1.1), (12345, 4.12)]
temp_dict[2002] = [(12345, 1.1), (12345, 7.11)]
temp_dict[2003] = [(12345, 1.1), (12345, 10.10)]
temp_dict[2004] = [(12345, 1.1), (12345, 30.15)]

#sort temperatures descending by max temp 
items = temp_dict.items()               
items.sort(key=lambda x: x[1][1], reverse=True)                  

for i in items:
    #python will convert \n to os.linesep
    print('%s,%s,%s,%s,%s' % (i[0], i[1][0][0], i[1][0][1], i[1][1][0], i[1][1][1]))   