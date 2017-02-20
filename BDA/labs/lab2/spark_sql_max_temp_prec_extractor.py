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
inFile = sc.textFile(iFile).map(lambda line: line.split(";")) \
                                      .map(lambda obs: Row(station=int(obs[0]),
                                                           temp=float(obs[3])))

tempSchema = sqlContext.createDataFrame(inFile)
tempSchema.registerTempTable("TempSchema")

# precipitation
inFile2 = sc.textFile(iFile2).map(lambda line: line.split(";")) \
                                          .map(lambda obs: Row(station=int(obs[0]),
                                                               day=obs[1],
                                                               precip=float(obs[3])))

precSchema = sqlContext.createDataFrame(inFile2)
precSchema.registerTempTable("PrecSchema")

combinedTempPrec = sqlContext.sql(
        """
        SELECT tr.station, MAX(temp) AS max_temp, MAX(precip) AS max_precip
        FROM
        TempSchema AS tr
        INNER JOIN
        (
        SELECT station, SUM(precip) AS precip
        FROM PrecSchema
        GROUP BY day, station
        ) AS pr
        ON tr.station = pr.station
        WHERE temp >= 25 AND temp <= 30
        AND precip >= 100 AND precip <= 200
        GROUP BY tr.station
        ORDER BY tr.station DESC
        """
    )

combinedTempPrec.rdd.repartition(1).sortBy(ascending=False, 
                        keyfunc=lambda (station, temp, precip): station)

combinedTempPrec.saveAsTextFile(oFile)
