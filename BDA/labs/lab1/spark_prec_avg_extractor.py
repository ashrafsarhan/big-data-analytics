#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext

iFile = 'data/stations-Ostergotland.csv'
iFile2 = 'data/precipitation-readings.csv'
oFile = 'data/OstergotlandAveMonthlyPrec'

sc = SparkContext(appName="OstergotlandAveMonthlyPrecSparkJob")

ostergotlandStations = sc.textFile(iFile)

ostergotlandStations = ostergotlandStations.map(lambda line: line.split(";")).map(lambda x: int(x[0])).distinct().collect()

isOstergotlandStation = (lambda s: s in ostergotlandStations)

precipitations = sc.textFile(iFile2)

daily_precipitations = precipitations.map(lambda line: line.split(";")).filter(lambda x: isOstergotlandStation(int(x[0])))

daily_precipitations = daily_precipitations.map(lambda x: (x[0]+','+x[1], float(x[3]))).reduceByKey(lambda a, b: a + b)

monthly_precipitations = daily_precipitations.map(lambda x:(x[0].split("-")[0]+','+x[0].split("-")[1], (x[1], 1)))

monthly_precipitations = monthly_precipitations.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))

monthly_precipitations = monthly_precipitations.map(lambda x: (x[0], x[1][0] / x[1][1]))

monthly_precipitations_csv = monthly_precipitations.map(lambda a: '%s,%s' % (a[0], a[1]))

monthly_precipitations_csv.coalesce(1).saveAsTextFile(oFile)
