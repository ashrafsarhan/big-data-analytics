#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/temperature-readings.csv'
iFile2 = 'data/precipitation-readings.csv'
oFile = 'data/sql_max_temperature_precipitation'

sc = SparkContext(appName="MaxTempPrecExtractorSparkSQLJob")

sqlContext = SQLContext(sc)

# Temperatures
inFile = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .map(lambda l: \
                Row(station = l[0], \
                    date = l[1],  \
                    year = l[1].split("-")[0], \
                    month = l[1].split("-")[1], \
                    day = l[1].split("-")[2], \
                    time = l[2], \
                    temp = float(l[3]), \
                    quality = l[4]))

tempSchema = sqlContext.createDataFrame(inFile)

tempSchema.registerTempTable("TempSchema")

# precipitation
inFile2 = sc.textFile(iFile2) \
            .map(lambda line: line.split(";")) \
            .map(lambda l: \
                Row(station = l[0], \
                    date = l[1],  \
                    year = l[1].split("-")[0], \
                    month = l[1].split("-")[1], \
                    day = l[1].split("-")[2], \
                    time = l[2], \
                    prec = float(l[3]), \
                    quality = l[4]))

precSchema = sqlContext.createDataFrame(inFile2)

precSchema.registerTempTable("PrecSchema")

tempPrec = sqlContext.sql(" \
                SELECT t.station, \
                        MAX(t.temp) AS maxTemp, \
                        MAX(p.dailyPrec) AS maxDailyPrec\
                FROM TempSchema t, \
                    (SELECT station, SUM(prec) as dailyPrec \
                    FROM PrecSchema \
                    GROUP BY station, day) p \
                WHERE t.station = p.station AND \
                        t.temp >= 25 AND t.temp <= 30 AND \
                        p.dailyPrec >= 100 AND p.dailyPrec <= 200 \
                GROUP BY t.station")

tempPrec.saveAsTextFile(oFile)
