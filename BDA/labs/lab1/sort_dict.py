#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 10:31:18 2016

@author: ashraf
"""
#import argparse

#parser = argparse.ArgumentParser(description='sort_dic')
#parser.add_argument('-o', '--output', help='Output file name', default='stdout')
#requiredNamed = parser.add_argument_group('Required arguments')
#requiredNamed.add_argument('-i', '--input', help='Input file name', required=True)
#parser.parse_args(['-h'])

temp_dict = dict()
#1st tuple for min temp and 2nd tuple for max temp
temp_dict[2005] = [[191900,-38.2],[98210,33.3]]
temp_dict[2006] = [[192830,-49.0],[98210,32.4]]
temp_dict[2007] = [[179960,-40.7],[98210,31.6]]
temp_dict[2008] = [[160790,-41.2],[97260,32.2]]
temp_dict[2009] = [[192840,-42.5],[96560,34.4]]
temp_dict[2010] = [[155790,-41.7],[96140,30.8]]
temp_dict[2011] = [[179950,-42.5],[94180,31.8]]
temp_dict[2012] = [[113410,-42.2],[94050,33.8]]
temp_dict[2013] = [[169860,-40.7],[86420,32.2]]
temp_dict[2014] = [[157860,-37.0],[86200,36.1]]


#sort temperatures descending by max temp 
items = temp_dict.items()               
items.sort(key=lambda x: x[1][1][1], reverse=True)                  

for i in items:
    #python will convert \n to os.linesep
    print('%s,%s,%s,%s,%s' % (i[0], i[1][0][0], i[1][0][1], i[1][1][0], i[1][1][1]))   
    
#write the output to file.
with open('test.csv','wb+') as f:
    for i in items:
        f.write('%s,%s,%s,%s,%s\n' % (i[0], i[1][0][0], i[1][0][1], i[1][1][0], i[1][1][1]))
#close the file after writting the lines.  
f.close()