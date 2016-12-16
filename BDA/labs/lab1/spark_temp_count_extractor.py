#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext

iFile = 'data/temperature-readings.csv'
oFile = 'data/over_ten_mth_temp_counts'
oFile2 = 'data/over_ten_temp_distinct_counts'
fromYear = 1950
toYear = 2014
target_temp = 10

sc = SparkContext(appName="TempCounterSparkJob")

lines = sc.textFile(iFile)

lines = lines.map(lambda a: a.split(";"))

lines = lines.filter(lambda x: int(x[1][0:4]) >= fromYear and int(x[1][0:4]) <= toYear and float(x[3]) > target_temp)

overTenMthTemp = lines.map(lambda x: (x[1][5:7], 1))

overTenMthTempCounts = overTenMthTemp.reduceByKey(lambda v1,v2: v1 + v2)

overTenMthTempCountsCsv = overTenMthTempCounts.map(lambda a: '%s,%s' % (a[0], a[1]))

overTenMthTempCountsCsv.coalesce(1).saveAsTextFile(oFile)

###################### Distinct Counting Per Station ######################
overTenStationTemp = lines.map(lambda x: (x[0], (x[1][5:7], float(x[3]))))

overTenStationTempDistinct = overTenStationTemp.distinct()

overTenStationTempDistinctCounts = overTenStationTempDistinct.map(lambda x: (x[0], 1)).reduceByKey(lambda v1,v2: v1 + v2)

overTenStationTempDistinctCountsCsv = overTenStationTempDistinctCounts.map(lambda a: '%s,%s' % (a[0], a[1]))

overTenStationTempDistinctCountsCsv.coalesce(1).saveAsTextFile(oFile2)


