#!/usr/bin/env python
"""product-behav-pattern-mining.py: Identifies important patterns in usage 
   behavior and margin."""

__author__     = "Sanjay Kumar"
__copyright__  = "Flytxt Mobile Solutions"
__license__    = "GPL"
__version__    = "0.0.1"
__email__      = "sanjay.kumar@flytxt.com"
__status__     = "Experimental"


###############################################################################

import sys
import re
import logging
from pyspark import SparkContext, SparkConf
from numpy.lib.function_base import add
from datetime import datetime
from numpy import append


# Functions to read data from specific colums 
def read_data(rdd,max_col_index=0,delimiter=',',num_cols=0,cols_list=[], \
			  cols_type=[],filter_invalid=True):
    if delimiter == '':
        return rdd
    else:
        if filter_invalid:
            return rdd.map(lambda z:create_cols(z,max_col_index,delimiter,\
            			   num_cols,cols_list,cols_type))\
        .filter(lambda z:z!='INVALID')
        else:
            return rdd.map(lambda z:create_cols(z,max_col_index,delimiter,\
            			   num_cols,cols_list,cols_type))

def create_cols_from_list_without_type(temp,cols_list):
    return tuple([temp[i] for i in cols_list])

def create_cols(row,max_col_index,delimiter,num_cols,cols_list,cols_type):
    temp = row.split(delimiter)
    if len(temp)<num_cols:
        return "INVALID"
    if len(temp)==1:
        return temp[0]
    if len(temp)<=max_col_index:
        return "INVALID"
    if len(cols_list)==0:
        return tuple(temp)
    if len(cols_list)!=num_cols:
        return tuple(temp[:num_cols])
    else:
        if len(cols_list)!=len(cols_type):
            return create_cols_from_list_without_type(temp,cols_list)
        else:
            return create_cols_from_list_with_type(temp,cols_list,cols_type)

# Function to check number being parsed
def numCheck(num):
    if len(num) < 10 :
        return "0000000000"
    if len(num) > 10 :
        return num[(len(num)-10):len(num)]
    return num

sc = SparkContext("spark://master:7077",appName="product-behav-pattern-mining")

# Identify set of subscribers who have made recharges from 27th Nov to 3rd of 
# Dec but no recharges X days prior to and after it. Choice of X = 30 as most
# products have a typical validity of 30 days. The filter criteria to ignore
# all of subscribers who have recharged in between the pre and post X day 
# periods is because measuring the margin impact of the products becomes fairly
# complicated otherwise. 


# Initial cleaning of inputs to get rdds in required format for parsing. At the
# end of this step we should have required rdds for further processing

#This path contains files of <MSISDN, ARPU, AON> tuples
path_msisdn_info = "/user/hduser/sanjay/MP/Output/MSISDN_MASTER_FILE/Month_1/*"
#This path contains recharge event files
path_rchg_info   = "/user/hduser/sanjay/MP/MP/recharge/MP_DFE_Recharge_event_*"

#rchg data contains <number, date, value> tuples
filter_dates = ["28-10-14","29-10-14","30-10-14","31-10-14","1-11-14","2-11-14"\
                ,"3-11-14","4-11-14"]
d = sc.textFile(path_rchg_info).coalesce(100)
rchg_data = read_data(d,max_col_index=2,delimiter=',',num_cols=3,
            cols_list=[0,2,3])\
            .map(lambda x:(numCheck(x[0]),x[1].split(" ")[0],x[2]))\
            .filter(lambda x: (x[1].split('-')[1] in ['10','11']))

#Filter subscribers who have AON > 120 days and broadcast the list to all nodes 
msisdn = sc.textFile(path_msisdn_info).map(lambda x:x.split(','))\
         .filter(lambda x:(int(x[2]) >=120)).map(lambda x:(x[0],0))
m = sc.broadcast(msisdn.collectAsMap())

filtered_msisdns =  rchg_data.map(lambda x: x[0],(0 if x[1] in filter_dates \
                    else 1)).reduceByKey(add).filter(lambda x: (x[0] in m.value)\
                    & (x[1] == 0))
fm_bc = sc.broadcast(filtered_msisdns.collectAsMap())

fm_rchg_data = rchg_data.filter(lambda x: x[0] in fm_bc).map(lambda x:\
                (x[0],x[2])).reduceByKey(append)

# local_og_data  = 
# std_og_data =
# onnet_og_data = 
# offnet_og_data = 
# sms_data =
# internet_data = 
# ic_data = 




# Identify associated prepost behaviors of subscribers mentioned from previous
# step. 

 

# Mine frequent patterns and association rules using FP growth algorithm to 
# identify recurring patterns

