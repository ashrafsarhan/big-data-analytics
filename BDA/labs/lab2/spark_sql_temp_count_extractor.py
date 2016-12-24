#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 17:06:24 2016

@author: ashraf
"""
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

iFile = 'data/temperature-readings.csv'
oFile1 = 'data/sql_over_ten_mth_temp_counts'
oFile2 = 'data/sql_over_ten_temp_distinct_counts'


sc = SparkContext(appName = "TempCounterSparkSQLJob")

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

overTenTemp = sqlContext.sql(" \
                        SELECT FIRST(year) AS year, FIRST(month) AS month, COUNT(temp) AS counts\
                        FROM TempSchema \
                        WHERE temp >= 10 AND year >= 1950 AND year <= 2014\
                        GROUP BY year, month \
                        ORDER BY counts DESC")

overTenTemp.saveAsTextFile(oFile1)


overTenTempDistinct = sqlContext.sql(" \
                        SELECT FIRST(year) AS year, FIRST(month) AS month, FIRST(station) AS station, \
                                COUNT(DISTINCT temp) AS counts\
                        FROM TempSchema \
                        WHERE temp >= 10 AND year >= 1950 AND year <= 2014\
                        GROUP BY year, month, station \
                        ORDER BY counts DESC")


overTenTempDistinct.saveAsTextFile(oFile2)

