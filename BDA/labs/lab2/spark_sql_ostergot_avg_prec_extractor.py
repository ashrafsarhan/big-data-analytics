#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/stations-Ostergotland.csv'
iFile2 = 'data/precipitation-readings.csv'
oFile = 'data/OstergotlandAveMonthlyPrec'

sc = SparkContext(appName="OstergotlandAvgMonthlyPrecSparkSQLJob")

sqlContext = SQLContext(sc)


# Ostergotland Stations
ostergotlandStations = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .map(lambda l: int(l[0])) \
            .distinct().collect()

isOstergotlandStation = (lambda s: s in ostergotlandStations)

inFile = sc.textFile(iFile2) \
            .map(lambda line: line.split(";")) \
            .filter(lambda l: isOstergotlandStation(int(l[0])) \
            .map(lambda l: \
                Row(station = l[0], \
                    date = l[1],  \
                    year = l[1].split("-")[0], \
                    month = l[1].split("-")[1], \
                    day = l[1].split("-")[2], \
                    time = l[2], \
                    prec = float(l[3]), \
                    quality = l[4]))

precSchema = sqlContext.createDataFrame(inFile)

precSchema.registerTempTable("PrecSchema")

avgPrec = sqlContext.sql(" \
                    SELECT ps.year, ps.month, AVG(prec.totPrec) AS avgMonthPrec \
                    FROM PrecSchema ps, \
                        (SELECT year, month, station, SUM(prec) AS totPrec \
                        FROM PrecSchema  \
                        GROUP BY year, month, station) AS prec \
                    WHERE ps.station = prec.station AND \
                           ps.year = prec.year AND \
                            ps.month = prec.month AND \
                            ps.year >= 1993 AND ps.year <= 2016 \
                    GROUP BY ps.year, ps.month")

avgPrec = avgPrec.rdd.repartition(1) \
                .sortBy(ascending = False, keyfunc = lambda \
                    (year, month, prec): (year, month))

avgPrec.saveAsTextFile(oFile)
