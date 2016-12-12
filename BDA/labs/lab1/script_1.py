#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 11 21:27:34 2016

@author: ashraf
"""
import time, argparse

parser = argparse.ArgumentParser(description='MinMaxTempExtractor')
parser.add_argument('-o', '--output', help='Output file', default='out_temp.csv')
requiredNamed = parser.add_argument_group('Required arguments')
requiredNamed.add_argument('-t', '--time', help='Time interval in years ex. 2005:2010', required=True)
requiredNamed.add_argument('-i', '--input', help='Input file', required=True)
args = parser.parse_args()
iFile = args.input
oFile = args.output
years =  args.time.split(":")
min_year = int(years[0])
max_year = int(years[1])

start = time.time()
print('Running Python MinMaxTempExtractor:\nFrom %s To %s\nInput file: %s\nOutput file: %s' 
      % (min_year, max_year, iFile, oFile))
temp_dict = dict()
with open(iFile) as f:
    for l in f:
        line = l.split(";")
        year = int(line[1].split("-")[0])
        if year >= min_year and year <= max_year:
            temp = temp_dict.get(year)
            station = line[0];
            curr_temp = float(line[3]);
            if not temp:
                #1st list for min temp and 2nd list for max temp
                temp_dict[year] = [[station, curr_temp], [station, curr_temp]]
            else:
                min = float(temp[0][1])
                max = float(temp[1][1])
                if curr_temp < min:
                    temp[0][0] = station
                    temp[0][1] = curr_temp
                if curr_temp > max:
                    temp[1][0] = station
                    temp[1][1] = curr_temp
#close the file after reading the lines.
f.close() 

#sort temperatures descending by max temp 
sorted_temp = temp_dict.items()               
sorted_temp.sort(key=lambda x: x[1][1][1], reverse=True)  

#write the output to file.
with open(oFile,'wb+') as f:
    for i in sorted_temp:
        #python will convert \n to os.linesep
        f.write('%s,%s,%s,%s,%s\n' % (i[0], i[1][0][0], i[1][0][1], i[1][1][0], i[1][1][1]))
#close the file after writting the lines.  
f.close()
end = time.time()
print('Done in %s seconds' % (end - start))
      