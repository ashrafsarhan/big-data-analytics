#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 10:31:18 2016

@author: ashraf
"""
temp_dict = dict()
temp_dict[2000] = {'min':{'station':12345, 'value':1.1}, 
                'max':{'station':12345, 'value':2.1}}
temp_dict[2001] = {'min':{'station':12345, 'value':1.1}, 
                'max':{'station':12345, 'value':3.3}}
temp_dict[2002] = {'min':{'station':12345, 'value':1.1}, 
                'max':{'station':12345, 'value':4.0}}
temp_dict[2003] = {'min':{'station':12345, 'value':1.1}, 
                'max':{'station':12345, 'value':6.0}}
temp_dict[2004] = {'min':{'station':12345, 'value':1.1}, 
                'max':{'station':12345, 'value':7.12}}

temp_dict = hash(temp_dict)

print temp_dict

#sort temperatures descending by max temp
#get('max').get('value')
#sorted_temp = sorted(temp_dict.items(), key=lambda x: temp_dict[x]['max']['value'])
#sorted_temp = sorted(mydict.iteritems(), key=lambda (k,v): (v,k)):
#for k, v in temp_dict.items():
#    print('%s, %s' %(k, v))

