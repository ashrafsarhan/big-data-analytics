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
fromYear = 1960
toYear = 2014

sc = SparkContext(appName="OstergotlandAvgMonthlyTempDiffSparkJob")

ostergotlandStations = sc.textFile(iFile)

ostergotlandStations = ostergotlandStations.map(lambda line: line.split(";")).map(lambda x: int(x[0])).distinct().collect()

isOstergotlandStation = (lambda s: s in ostergotlandStations)

temperatures = sc.textFile(iFile2).map(lambda line: line.split(";")). \
filter(lambda x: isOstergotlandStation(int(x[0])) and int(x[1][0:4]) >= fromYear and int(x[1][0:4]) <= toYear)

monthlyAvgTemps = temperatures.map(lambda obs:
                                             ((obs[1], int(obs[0])),
                                                    (float(obs[3]), float(obs[3])))) \
                                              .reduceByKey(lambda (mint1, maxt1), (mint2, maxt2):
                                                           (min(mint1, mint2), max(maxt1, maxt2))) \
                                              .map(lambda ((day, station), (mint, maxt)):
                                                   (day[:7], (mint + maxt, 2))) \
                                              .reduceByKey(lambda (temp1, count1), (temp2, count2):
                                                           (temp1 + temp2, count1 + count2)) \
                                              .map(lambda (month, (temp, count)):
                                                   (month, temp / float(count)))

monthlyLongtermAvgTemps = monthlyAvgTemps.filter(lambda (month, temp):
                                                    int(month[:4]) <= 1980) \
                                            .map(lambda (month, temp):
                                                 (month[-2:], (temp, 1))) \
                                            .reduceByKey(lambda (temp1, count1), (temp2, count2):
                                                         (temp1 + temp2, count1 + count2)) \
                                            .map(lambda (month, (temp, count)):
                                                 (month, temp / float(count)))

month_temp = {month: temp for month, temp in monthlyLongtermAvgTemps.collect()}

monthlyAvgTemps = monthlyAvgTemps.map(lambda (month, temp):
                                        (month, abs(temp) - abs(month_temp[month[-2:]]))) \
                                   .sortBy(ascending=True, keyfunc=lambda (month, temp): month)

monthlyAvgTempsCsv = monthlyAvgTemps.map(lambda a: '%s,%s,%s' % (a[0].split('-')[0], a[0].split('-')[1], a[1]))

monthlyAvgTempsCsv.repartition(1).saveAsTextFile(oFile)


