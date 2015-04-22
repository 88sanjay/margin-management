#!/usr/bin/env python
"""product-behav-pattern-mining.py: Identifies important patterns in usage 
   behavior and margin."""

__author__     = "Sanjay Kumar"
__copyright__  = "Flytxt Mobile Solutions"
__license__    = "Flytxt Mobile Solutions"
__version__    = "0.0.1"
__email__      = "sanjay.kumar@flytxt.com"
__status__     = "Experimental"


###############################################################################

import sys
import re
import logging
from pyspark import SparkContext, SparkConf
from numpy.lib.function_base import add
from datetime import date
from numpy import append
from datetime import date

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
def num_check(num):
    if len(num) < 10 :
        return "0000000000"
    if len(num) > 10 :
        return num[(len(num)-10):len(num)]
    return num


def get_week(date_str,date_format):
    if date_format == "MM-DD-YY":
        delta = (date(2014,int(date_str.split('-')[0]),int(date_str.split('-')[1]))\
                - date(2014,11,1)).days
    else:
        delta = -999
    return int(delta/7)

def map_to_array(idx,val,length=7):
    s = [0 for i in range(0,length)]
    s[idx+int(length/2)] = val # to correct for negative values of week passed
    return s

#Function to create RDD of behavioral KPIs
def aggregate_week_month(rdd,MSISDN_idx,KPI_idx,date_idx,date_format):
    return rdd.map(lambda x:(x[MSISDN_idx],get_week(x[date_idx],date_format),\
        (float(x[KPI_idx]) if x[KPI_idx] else 0.0))).filter(lambda x: \
        ((x[1] < 4) & (x[1] > -4))).map(lambda x: (x[0],map_to_array(x[1],x[2])\
        )).reduceByKey(add).map(lambda x: x[0]+","+",".join(map(str,x[1])))

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
filter_dates = ["2014-10-28","2014-10-29","2014-10-30","2014-10-31","2014-11-01"\
                ,"2014-11-02","2014-11-03","2014-11-04"]
d = sc.textFile(path_rchg_info).coalesce(100)
rchg_data = read_data(d,max_col_index=2,delimiter=',',num_cols=3,
            cols_list=[0,2,3])\
            .map(lambda x:(num_check(x[0]),x[1].split(" ")[0],x[2]))\
            .filter(lambda x: (x[1].split('-')[1] in ['10','11']))

#Filter subscribers who have AON > 120 days and broadcast the list to all nodes 
filtered_msisdns =  rchg_data.map(lambda x: (x[0],(0 if x[1] in filter_dates \
                    else 1))).reduceByKey(add).filter(lambda x: (x[1] == 0))
fm_bc = sc.broadcast(filtered_msisdns.collectAsMap())
msisdn = sc.textFile(path_msisdn_info).map(lambda x:x.split(','))\
         .filter(lambda x:(int(x[2]) >=120) & (x[0] in fm_bc.value))\
         .map(lambda x:(x[0],0))
msisdn_bc = sc.broadcast(msisdn.collectAsMap())
fm_bc.unpersist()
fm_rchg_data = rchg_data.filter(lambda x: x[0] in msisdn_bc.value).map(lambda x:\
                (x[0],x[2])).reduceByKey(append).map(lambda x: x[0]+","+\
                ";".join(map(str,x[1])))

# Create data sets with other KPIs from OG MOU CDR 
MSIDN_index=0
og_data_path = '/user/hduser/sanjay/MP/MP/og_voice_usage/'
ogmou_data = sc.textFile(og_data_path).coalesce(120).map(lambda x: x.split(','))\
            .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])] + \
            x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
            msisdn_bc.value).persist(StorageLevel.MEMORY_AND_DISK)

local_og_data  = aggregate_week_month(ogmou_data,0,23,1,'MM-DD-YY')
std_og_data = aggregate_week_month(ogmou_data,0,4,1,'MM-DD-YY')
onnet_og_data = aggregate_week_month(ogmou_data,0,3,1,'MM-DD-YY')
isd_og_data =  aggregate_week_month(ogmou_data,0,19,1,'MM-DD-YY')
total_og_data = aggregate_week_month(ogmou_data,0,13,1,'MM-DD-YY')

sms_data = aggregate_week_month(ogmou_data,0,2,1,'MM-DD-YY')

# internet_data = 

# ic_data = 

# decrement


# Identify associated prepost behaviors of subscribers mentioned from previous
# step. 

 

# Mine frequent patterns and association rules using FP growth algorithm to 
# identify recurring patterns

