#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/temperature-readings.csv'
oFile1 = 'data/sql_max_temperature'
oFile2 = 'data/sql_min_temperature'

sc = SparkContext(appName = "MinMaxTempExtractorSparkSQLJob")

sqlContext = SQLContext(sc)

inFile = sc.textFile(iFile).map(lambda line: line.split(";")). \
map(lambda l: Row(station = l[0], year = l[1].split("-")[0], month = l[1].split("-")[1], day = l[1].split("-")[2], time = l[2], temp = float(l[3]), quality = l[4]))

tempSchema = sqlContext.createDataFrame(inFile)

tempSchema.registerTempTable("TempSchema")

minTemp = sqlContext.sql("SELECT FIRST(year) AS year, FIRST(station) AS station, MIN(temp) AS minTemp \
                          FROM TempSchema \
                          WHERE year >= 1950 AND year <= 2014 \
                          GROUP BY year \
                          ORDER BY minTemp DESC")

minTemp.coalesce(1).saveAsTextFile(oFile1)

maxTemp = sqlContext.sql("SELECT FIRST(year) AS year, FIRST(station) AS station, MAX(temp) AS maxTemp \
                          FROM TempSchema \
                          WHERE year >= 1950 AND year <= 2014 \
                          GROUP BY year \
                          ORDER BY maxTemp DESC")

maxTemp.coalesce(1).saveAsTextFile(oFile2)

