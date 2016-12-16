#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext

iFile = 'data/temperature-readings.csv'
oFile = 'data/station_avg_mth_temp'
fromYear = 1960
toYear = 2014

sc = SparkContext(appName="AvgTempSparkJob")

lines = sc.textFile(iFile)

lines = lines.map(lambda a: a.split(";"))

lines = lines.filter(lambda x: int(x[1][0:4]) >= fromYear and int(x[1][0:4]) <= toYear)

temperatures = lines.map(lambda x: (x[0]+','+x[1][5:7], (float(x[3]), 1)))

stationMthTemp = temperatures.reduceByKey(lambda v1,v2: (v1[0]+v2[0], v1[1]+v2[1]))

stationAvgMthTemp = stationMthTemp.map(lambda a: (a[0], a[1][0]/a[1][1]))

stationAvgMthTempCsv = stationAvgMthTemp.map(lambda a: '%s,%s' % (a[0], a[1]))

stationAvgMthTempCsv.coalesce(1).saveAsTextFile(oFile)
