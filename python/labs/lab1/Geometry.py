#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  6 17:15:38 2016
Geometry module
@author: ashraf
"""
import math

def CircleArea(radius):
    if radius > 0:
      a = radius**2 * math.pi
      return a
    else:
      print('Fetal error: The radius must be positive')
      return None

def RectangleArea(base,height):
    a = base * height
    return a
