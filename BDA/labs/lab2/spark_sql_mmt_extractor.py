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

inFile = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .map(lambda obs: \
                Row(station = obs[0], date = obs[1],  \
                    year = obs[1].split("-")[0], time = obs[2],
                    temp = float(obs[3]), quality = obs[4]))

tempSchema = sqlContext.createDataFrame(inFile)

tempSchema.registerTempTable("TempSchema")

"""
Q1. year, station with the max, maxValue ORDER BY maxValue DESC
"""
maxTemp = sqlContext.sql("""
        SELECT DISTINCT(table1.year) AS year,
                FIRST(table1.station) AS station,
                FIRST(temp) AS temp
        FROM TempSchema AS table1
        INNER JOIN
        (
        SELECT year, MAX(temp) AS max_temp
        FROM TempSchema
        GROUP BY year
        ) AS table2
        ON table1.year = table2.year
        WHERE table1.temp = table2.max_temp
        GROUP BY table1.year
        ORDER BY temp DESC
        """
        )

maxTemp = maxTemp.rdd.repartition(1)\
                    .sortBy(ascending = False, keyfunc = lambda \
                            (year, station, temp): temp)

print maxTemp.take(10)
maxTemp.saveAsTextFile(oFile1)

"""
Q2. year, station with the min, minValue ORDER BY minValue DESC
"""
minTemp = sqlContext.sql("""
        SELECT DISTINCT(table1.year) AS year,
                FIRST(table1.station) AS station,
                FIRST(temp) AS temp
        FROM TempSchema AS table1
        INNER JOIN
        (
        SELECT year, MIN(temp) AS min_temp
        FROM TempSchema
        GROUP BY year
        ) AS table2
        ON table1.year = table2.year
        WHERE table1.temp = table2.min_temp
        GROUP BY table1.year
        ORDER BY temp DESC
        """
)

minTemp = minTemp.rdd.repartition(1)\
                    .sortBy(ascending = False, keyfunc = lambda \
                            (year, station, temp): temp)
print minTemp.take(10)
minTemp.saveAsTextFile(oFile2)

