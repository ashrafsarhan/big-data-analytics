#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Kernel Function

@author: ashraf
"""

from pyspark import SparkContext

sc = SparkContext(appName = "kernel")

sc.addFile("/nfshome/x_asmoh/py_scripts/help_functions.py")

import help_functions as hfunc

# Station, lat, long
stations = sc.textFile("data/stations.csv") \
                .map(lambda line: line.split(";")) \
                .map(lambda obs: (obs[0], (float(obs[3]),float(obs[4])))) \
                .collect()

stations_dict = {}
for s in stations:
    stations_dict[s[0]] = s[1]

#Broadcast stations_dict
stations_bc = sc.broadcast(stations_dict)

# (station, (date, time, temp))
temperatures = sc.textFile("data/temperature-readings.csv") \
                    .sample(False, .0001, 12345) \
                    .map(lambda line: line.split(";")) \
                    .map(lambda l: \
                        (l[0], (str(l[1]), str(l[2]), float(l[3]))))

def mergeVal(x):
    sVals = list(stations_bc.value[x[0]])
    vals = list(x[1])
    vals.extend(sVals)
    return (x[0],tuple(vals))

def kernelFunc(pred, data, dist):
    import datetime
    result = list()
    for p in pred:
        temp = data \
            .filter(lambda x: \
                datetime.datetime.strptime(x[1][0], '%Y-%m-%d') < \
                datetime.datetime.strptime(p[1], '%Y-%m-%d')) \
            .map(lambda x: \
                (x[1][2], ( \
                        hfunc.deltaHours(p[0],x[1][1]), \
                        hfunc.deltaDays(p[1], x[1][0]), \
                        hfunc.haversine(lon1 = p[2], \
                                            lat1 = p[3], \
                                            lon2 = x[1][4], \
                                            lat2 = x[1][3])))) \
            .map(lambda (temp, (distTime, distDays, distKM)): \
                (temp,(hfunc.gaussian(distTime, h = dist[0]), \
                        hfunc.gaussian(distDays, h = dist[1]), \
                        hfunc.gaussian(distKM, h = dist[2])))) \
            .map(lambda (temp, (ker1, ker2, ker3)): \
                (temp,ker1 + ker2 + ker3)) \
            .map(lambda (temp, kerSum): \
                (temp, (kerSum, temp*kerSum))) \
            .map(lambda (temp, (kerSum, tkSum)): \
                (None, (kerSum, tkSum))) \
            .reduceByKey(lambda (kerSum1, tkSum1), (kerSum2, tkSum2): \
                (kerSum1 + kerSum2, tkSum1 + tkSum2)) \
            .map(lambda (key,(sumKerSum, sumTkSum)): \
                (float(sumTkSum)/float(sumKerSum)))
        result.append((p[0], temp.collect()))
    return result

# Test the kernelFunc 
# (station, (date, time, temp, lat, long))
train = temperatures.map(lambda l: mergeVal(l))    
pred = (('04:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('06:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('08:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('10:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('12:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('14:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('16:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('18:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('20:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('22:00:00', '2013-01-25',float(15.62), float(58.41)),
        ('00:00:00', '2013-01-25',float(15.62), float(58.41)))
dist = (2, 7, 100)
rsltPred = kernelFunc(pred, train, dist)
rsltPred_rdd = sc.parallelize(rsltPred).repartition(1)
rsltPred_rdd.take(10)
