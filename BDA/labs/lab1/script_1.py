#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 11 21:27:34 2016

@author: ashraf
"""
import sys, time

start = time.time()
min_year = int(sys.argv[1])
max_year = int(sys.argv[2])
file_in = sys.argv[3]
file_out = sys.argv[4]
print('Running python script for:\nmin_year: %s\nmax_year: %s\nInput file: %s\nOutput file: %s' 
      % (min_year, max_year, file_in, file_out))
temp_dict = dict()
with open(file_in) as f:
    for l in f:
        line = l.split(";")
        year = int(line[1].split("-")[0])
        if year >= min_year and year <= max_year:
            temp = temp_dict.get(year)
            station = line[0];
            curr_temp = float(line[3]);
            if not temp:
                temp_dict[year] = {'min':{'station':station, 'value':curr_temp}, 
                'max':{'station':station, 'value':curr_temp}}
            else:
                min = float(temp.get('min').get('value'))
                max = float(temp.get('max').get('value'))
                if curr_temp < min:
                    temp.get('min')['station'] = station
                    temp.get('min')['value'] = curr_temp
                if curr_temp > max:
                    temp.get('max')['station'] = station
                    temp.get('max')['value'] = curr_temp
#close the file after reading the lines.
f.close() 
#write the output to file.  
f = open(file_out,'wb+')
for k, v in temp_dict.items():
    #python will convert \n to os.linesep
    f.write('%s,%s,%s,%s,%s\n' % (k, v.get('min').get('station'), 
                               v.get('min').get('value'), 
                               v.get('max').get('station'), 
                               v.get('max').get('value')))
#close the file after writting the lines.  
f.close()
end = time.time()
print('Done in %s seconds' % (end - start))
      