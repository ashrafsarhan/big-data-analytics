#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/temperature-readings.csv'
oFile = 'data/sql_station_avg_mth_temp'
fromYear = 1960
toYear = 2014

sc = SparkContext(appName="AvgTempSparkSQLJob")

sqlContext = SQLContext(sc)

inFile = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .map(lambda l: \
                Row(station = l[0], date = l[1],  \
                    year = l[1].split("-")[0], \
                    month = l[1].split("-")[1], time = l[2], \
                    temp = float(l[3]), quality = l[4]))

tempSchema = sqlContext.createDataFrame(inFile)

tempSchema.registerTempTable("TempSchema")

avgMonthlyTemp = sqlContext.sql(" \
                        SELECT FIRST(year), FIRST(month), FIRST(station), \
                                AVG(temp) AS AvgTemp \
                        FROM TempSchema \
                        WHERE year >= 1960 AND year <= 2014 \
                        GROUP BY year, month, station \
                        ORDER BY AvgTemp DESC")

avgMonthlyTemp = avgMonthTemp.rdd.repartition(1) \
                            .sortBy(ascending = False, keyfunc = lambda \
                                (year, month, station, avgtemp): avgtemp)

avgMonthlyTemp.saveAsTextFile(oFile)
