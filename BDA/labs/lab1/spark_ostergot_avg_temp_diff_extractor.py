#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext

iFile = 'data/stations-Ostergotland.csv'
iFile2 = 'data/temperature-readings.csv'
oFile = 'data/OstergotlandAvgMonthlyDiffTemp'

sc = SparkContext(appName="OstergotlandAvgMonthlyTempDiffSparkJob")

ostergotlandStations = sc.textFile(iFile)

ostergotlandStations = ostergotlandStations.map(lambda line: line.split(";")).map(lambda x: int(x[0])).distinct().collect()

isOstergotlandStation = (lambda s: s in ostergotlandStations)

temperatures = sc.textFile(iFile2)

temperatures = temperatures.map(lambda line: line.split(";")).filter(lambda x: isOstergotlandStation(int(x[0])) and int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)

daily_temperatures = temperatures.map(lambda x: (x[1], (float(x[3]), float(x[3])))).reduceByKey(lambda t1, t2: (min(t1[0], t2[0]), max(t1[1], t2[1])))

monthly_temperatures = daily_temperatures.map(lambda x:(x[0].split("-")[0]+','+x[0].split("-")[1], (x[1][0]+x[1][1], 2)))

monthly_temperatures = monthly_temperatures.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))

monthly_avg_temperatures = monthly_temperatures.map(lambda x: (x[0], x[1][0] / x[1][1]))

longterm_monthly_avg_temperatures = monthly_avg_temperatures.filter(lambda x: int(x[0].split(",")[0]) >= 1950 and int(x[0].split(",")[0]) <= 1980).map(lambda x: (x[0].split(",")[1], (x[1], 1))).reduceByKey(lambda t1, t2:(t1[0] + t2[0], t1[1] + t2[1])).map(lambda t: (t[0], t[1][0]/t[1][1]))

longTermAvgTempLookup = {month: temp for month, temp in longterm_monthly_avg_temperatures.collect()}

final_res = monthly_avg_temperatures.map(lambda x: (x[0], x[1] - longTermAvgTempLookup[x[0].split(",")[1]]))

monthly_avg_diff_temperatures_csv = final_res.map(lambda a: '%s,%s' % (a[0], a[1]))

monthly_avg_diff_temperatures_csv.coalesce(1).saveAsTextFile(oFile)
