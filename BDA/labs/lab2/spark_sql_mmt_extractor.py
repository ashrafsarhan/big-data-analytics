#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/temperature-readings.csv'
oFile1 = 'data/sql_max_temperature_1'
oFile2 = 'data/sql_max_temperature_2'
oFile3 = 'data/sql_min_temperature_3'
oFile4 = 'data/sql_min_temperature_4'
fromYear = 1950
toYear = 2014

sc = SparkContext(appName = "MinMaxTempExtractorSparkSQLJob")

sqlContext = SQLContext(sc)

inFile = sc.textFile(iFile) \
            .map(lambda line: line.split(";")) \
            .filter(lambda obs:
                               (int(obs[1][:4]) >= fromYear and
                                int(obs[1][:4]) <= toYear)) \
            .map(lambda obs: \
                Row(station = obs[0], date = obs[1],  \
                    year = obs[1].split("-")[0], time = obs[2],
                    temp = float(obs[3]), quality = obs[4]))

tempSchema = sqlContext.createDataFrame(inFile)

tempSchema.registerTempTable("TempSchema")

"""
Q1.1 year, station with the max, maxValue ORDER BY maxValue DESC
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

maxTemp.take(10)

maxTemp.saveAsTextFile(oFile1)

"""
Another Solution
Q1.2 year, station with the max, maxValue ORDER BY maxValue DESC
"""
maxTemp2 = sqlContext.sql(
        """
        SELECT year, MAX(temp) AS temp
        FROM TempSchema
        GROUP BY year
        ORDER BY temp DESC
        """
    )

maxTemp2.rdd.repartition(1) \
		     .sortBy(ascending=False, keyfunc=lambda (year, temp): temp)

maxTemp2.take(10)

maxTemp2.saveAsTextFile(oFile2)

"""
Q2.1 year, station with the min, minValue ORDER BY minValue DESC
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

minTemp.take(10)

minTemp.saveAsTextFile(oFile3)

"""
Another Solution
Q2.2 year, station with the min, minValue ORDER BY minValue DESC
"""
minTemp2 = sqlContext.sql(
        """
        SELECT year, MIN(temp) AS temp
        FROM TempSchema
        GROUP BY year
        ORDER BY temp DESC
        """
    )

minTemp2.rdd.repartition(1) \
		     .sortBy(ascending=False, keyfunc=lambda (year, temp): temp)

minTemp2.take(10)

minTemp2.saveAsTextFile(oFile4)
