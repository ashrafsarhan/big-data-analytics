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
temperature_data = sc.textFile(iFile)
temperature_obs = temperature_data.map(lambda line: line.split(";")) \
                                      .map(lambda obs: Row(station=int(obs[0]),
                                                           temp=float(obs[3])))
schema_temp_readings = sqlContext.createDataFrame(temperature_obs)
schema_temp_readings.registerTempTable("temp_readings")

# precipitation
precipitation_data = sc.textFile(iFile2)
precipitation_obs = precipitation_data.map(lambda line: line.split(";")) \
                                          .map(lambda obs: Row(station=int(obs[0]),
                                                               day=obs[1],
                                                               precip=float(obs[3])))
schema_precip_readings = sqlContext.createDataFrame(precipitation_obs)
schema_precip_readings.registerTempTable("precip_readings")

combined = sqlContext.sql(
        """
        SELECT tr.station, MAX(temp) AS max_temp, MAX(precip) AS max_precip
        FROM temp_readings AS tr
        INNER JOIN
        (
        SELECT station, SUM(precip) AS precip
        FROM precip_readings
        GROUP BY day, station
        ) AS pr
        ON tr.station = pr.station
        WHERE (temp >= 25 AND temp <= 30)
        AND (precip >= 100 AND precip <= 200)
        GROUP BY tr.station
        ORDER BY tr.station DESC
        """
        )

tempPrec = combined.rdd.repartition(1) \
		.sortBy(ascending=False, keyfunc=lambda (station, temp, precip): station) 
     
tempPrec.take(10)

tempPrec.saveAsTextFile(oFile)
