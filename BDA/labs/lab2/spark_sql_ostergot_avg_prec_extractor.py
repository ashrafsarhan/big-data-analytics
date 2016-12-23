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

sc = SparkContext(appName="OstergotlandAvgMonthlyPrecSparkJob")

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext(appName = "E2A5")
sqlContext = SQLContext(sc)

# Accessing the stations

# Ostergotland Stations

stations = sc.textFile("/user/x_arbar/Data/stations-Ostergotland.csv") \
            .map(lambda line: line.split(";")) \
            .map(lambda obs: int(obs[0])) \
            .distinct().collect()

stations = {station: True for station in stations}

# Registering the table

# Temperatures
inFile = sc.textFile("/user/x_arbar/Data/precipitation-readings.csv") \
            .map(lambda line: line.split(";")) \
            .filter(lambda obs: stations.get(int(obs[0]), False)) \
            .map(lambda obs: \
                Row(station = obs[0], \
                    date = obs[1],  \
                    year = obs[1].split("-")[0], \
                    month = obs[1].split("-")[1], \
                    day = obs[1].split("-")[2], \
                    time = obs[2], \
                    prec = float(obs[3]), \
                    quality = obs[4]))

schemaPrecReadings = sqlContext.createDataFrame(inFile)

schemaPrecReadings.registerTempTable("precReadings")



# Query

avgPrec = sqlContext.sql(" \
                    SELECT pr.year, pr.month, AVG(prc.totPrec) AS avgMonthPrec \
                    FROM precReadings pr, \
                        (SELECT year, month, station, SUM(prec) AS totPrec \
                        FROM precReadings  \
                        GROUP BY year, month, station) AS prc \
                    WHERE pr.station = prc.station AND \
                            pr.year = prc.year AND \
                            pr.month = prc.month AND \
                            pr.year >= 1993 AND pr.year <= 2016 \
                    GROUP BY pr.year, pr.month")

avgPrec = avgPrec.rdd.repartition(1) \
                .sortBy(ascending = False, keyfunc = lambda \
                    (year, month, prec): (year, month))

print avgPrec.take(20)
avgPrec.saveAsTextFile("Results/Exercise2/E2A5")
