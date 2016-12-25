#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/stations-Ostergotland.csv'
iFile2 = 'data/temperature-readings.csv'
oFile = 'data/OstergotlandAveMonthlyDiffTemp'

sc = SparkContext(appName="OstergotlandAvgMonthlyTempDiffSparkSQLJob")

sqlContext = SQLContext(sc)


# Ostergotland Stations
ostergotlandStations = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .map(lambda l: int(l[0])) \
            .distinct().collect()

isOstergotlandStation = (lambda s: s in ostergotlandStations)

inFile = sc.textFile(iFile2) \
            .map(lambda line: line.split(";")) \
            .filter(lambda l: isOstergotlandStation(int(l[0]))) \
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


daily_temp = sqlContext.sql(" \
                    SELECT ts.station AS station, ts.date AS date, ts.year AS year, ts.month AS month, ts.day AS day, \
                           min.minTemp AS minTemp, max.maxTemp AS maxTemp \
                    FROM TempSchema ts \
                   (SELECT FIRST(station) AS station, FIRST(date) AS date, MIN(temp) minTemp \
                    FROM TempSchema ts\
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY station, date) AS min, \
                   (SELECT FIRST(station) AS station, FIRST(date) AS date, MAX(temp) maxTemp \
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY station, date) AS max \
                    WHERE ts.station = min.station = max.station AND ts.date = min.date = max.date")


daily_temp = sqlContext.sql(" \
                    SELECT min.station AS station, min.year AS year, min.month AS month, min.day AS day, AVG(min.minTemp + max.maxTemp) AS avgTemp \
                    FROM \
                   (SELECT FIRST(station) AS station, FIRST(year) AS year, FIRST(month) AS month, FIRST(day) AS day, MIN(temp) minTemp \
                    FROM TempSchema ts \
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY ts.date) AS min, \
                   (SELECT FIRST(station) AS station, FIRST(year) AS year, FIRST(month) AS month, FIRST(day) AS day, MAX(temp) maxTemp \
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY ts.date) AS max \
                    WHERE min.station == max.station AND min.year == max.year AND min.month == max.month AND min.day == max.day\
                    GROUP BY station, year, month, day")


daily_min_temp = sqlContext.sql(" \
                    SELECT FIRST(station) AS station, FIRST(year) AS year, FIRST(month) AS month, FIRST(day) AS day, MIN(temp) minTemp \
                    FROM TempSchema ts \
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY ts.date")

daily_max_temp = sqlContext.sql(" \
                    SELECT FIRST(station) AS station, FIRST(year) AS year, FIRST(month) AS month, FIRST(day) AS day, MAX(temp) maxTemp \
                    FROM TempSchema ts \
                    WHERE ts.year >= 1950 AND ts.year <= 1980 \
                    GROUP BY ts.date")

daily_temp = daily_min_temp.leftOuterJoin(daily_max_temp, (daily_min_temp.year == daily_max_temp.year) & (daily_min_temp.month == daily_max_temp.month) & (daily_min_temp.day == daily_max_temp.day))


avgPrec = avgPrec.rdd.repartition(1) \
                .sortBy(ascending = False, keyfunc = lambda \
                    (year, month, avgDiffTemp): (year, month))

avgPrec.saveAsTextFile(oFile)
