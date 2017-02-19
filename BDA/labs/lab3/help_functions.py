#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Help Function

@author: ashraf
"""
def gaussian(dist, h):
    import math, collections
    if isinstance(dist, collections.Iterable):
        res = []
        for x in dist:
            res.append(math.exp(float(-(x**2))/float((2*(h**2)))))
    else:
        res = math.exp(float(-(dist**2))/float((2*(h**2))))
    return res

def haversine(lon1, lat1, lon2,lat2, radians = 6371):
    import math
    # Convert decimal degrees to radians
    lon1, lat1, lon2,lat2 = map(math.radians, [lon1, lat1, lon2,lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat2
    a = math.sin(dlat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return c * radians

def timeCorr(time):
    import math
    result = []
    if hasattr(time, '__iter__'):
        for x in time:
            if x <= -12:
                result.append(24 + x)
            else:
                result.append(math.fabs(x))
    else:
        if time <= -12:
            result = 24 + time
        else:
            result = math.fabs(time)
    return result

def deltaHours(time1, time2):
    import datetime
    hDelta = datetime.datetime.strptime(time1, '%H:%M:%S')-datetime.datetime.strptime(time2, '%H:%M:%S')
    tDiff = hDelta.total_seconds()/3600
    tCorr = timeCorr(tDiff)
    return tCorr


def deltaDays(day1, day2):
    import datetime
    dDelta = datetime.datetime.strptime(day1, '%Y-%m-%d')-datetime.datetime.strptime(day2, '%Y-%m-%d')
    return dDelta.days

