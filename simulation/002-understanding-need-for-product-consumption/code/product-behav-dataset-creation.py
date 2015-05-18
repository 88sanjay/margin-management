#!/usr/bin/env python
"""product-behav-dataset-creation.py: Creation of data set for analysis 
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

month_filter = '11'

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

def get_kpi(x,arr_kpi_ix):
    out = []
    for i in arr_kpi_ix:
        out = (out + [float(x[i])]) if x[i] else (out + [0.0])
    return out


path_msisdn_info_MP = "/user/hduser/sanjay/MP/Output/MSISDN_MASTER_FILE/Month_1/*"
path_msisdn_info_KE = "/user/hduser/sanjay/KEL/MSISDN_MASTER/*"

#This path contains recharge event files
path_rchg_info_MP= "/user/hduser/sanjay/MP/MP/recharge/MP_DFE_Recharge_event_*"
path_rchg_info_KE= "/user/hduser/sanjay/KEL/vodafone_recharges/KE_Recharge_event_*"

#This path is for storing datasets
outpath = "/user/hduser/sanjay/margin-mgmt/"


tt_rchg_val_MP = ['6','7','5','10','20','21','30','42','47','50','60','67','70',\
                '80','81','87','100','111','200','222','225','250','270','333',\
                '400','500','750','1000','2000','3000']

tt_rchg_val_KL = ['5000','4000','3500','3000','2000','1001','1000','900','800',\
                '786','750','700','600','501','551','500','400','351','350','330',\
                '270','250','220','125','100',\
                '65','50','30','20','10','60','90','110','130','160','120']

path_rchg_info = path_rchg_info_KE
path_msisdn_info = path_msisdn_info_KE
#rchg_cols_list = [0,2,3]
rchg_cols_list = [0,1,2]
tt_rchg_val = tt_rchg_val_KL
circle = "KL"

d = sc.textFile(path_rchg_info).coalesce(100)


rchg_data = read_data(d,max_col_index=2,delimiter=',',num_cols=3,cols_list=\
            rchg_cols_list).map(lambda x:(num_check(x[0]),x[1].split(" ")[0],\
            str(int(float(x[2]))))).filter(lambda x: (x[1].split('-')[1] ==\
            month_filter) & (x[2] not in tt_rchg_val))

filtered_msisdns =  rchg_data.map(lambda x: (x[0],0)).distinct()

fm_bc = sc.broadcast(filtered_msisdns.collectAsMap())
msisdn = sc.textFile(path_msisdn_info).map(lambda x:x.split(','))\
         .filter(lambda x:(int(x[2]) >=120) & (x[0] in fm_bc.value))\
         .map(lambda x:(x[0],0))

msisdn_bc = sc.broadcast(msisdn.collectAsMap())
fm_bc.unpersist()
fm_rchg_data = rchg_data.filter(lambda x: x[0] in msisdn_bc.value).map(lambda x:\
                (x[0],x[2])).reduceByKey(lambda a,b: a+","+b)\
                .map(lambda x: (x[0],[i for i in set(x[1].split(','))]))\
                .map(lambda x: (x[0],["_".join(x[1])]))

# get leg wise aggregations for the month over various KPIs

MSIDN_index=0
#og_data_path = '/user/hduser/sanjay/MP/MP/og_voice_usage/'
#kpi_ix = [23,4,3,19,13,2]
og_data_path = '/KEL/vodafone_og_voice_usages/*'
kpi_ix = [13,3,5,9,6,2]
ogmou_data = sc.textFile(og_data_path).coalesce(120).map(lambda x: x.split(','))\
            .filter(lambda x: x[1].split('-')[0] == month_filter)\
            .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])] + \
            x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
            msisdn_bc.value).map(lambda x: (x[MSIDN_index],get_kpi(x,kpi_ix)))\
            .reduceByKey(add).map(lambda x: (x[0],map(str,x[1])))
 

# # Create data sets with other KPIs from Data CDR
MSIDN_index=0
#internet_data_path = "/user/hduser/sanjay/MP/MP/data_trigger/MP_Data_usage_"+\
#                    "trigger_2014*"

internet_data_path = "/KEL/vodafone_data_usages/KE_DATA_RECC_*"

#internet_usage = sc.textFile(internet_data_path).coalesce(120)\
#                .map(lambda x: x.split(','))\
#                .filter(lambda x: x[2].split('-')[1] == month_filter)\
#                .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                msisdn_bc.value).map(lambda x: (x[MSIDN_index],float(x[3])))\
#                .reduceByKey(add) 


internet_usage = sc.textFile(internet_data_path).coalesce(120)\
                 .map(lambda x: x.split(','))\
                 .filter(lambda x: x[1][4:6] == month_filter)\
                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
                 msisdn_bc.value).map(lambda x: (x[MSIDN_index],float(x[2])))\
                 .reduceByKey(add) 


# # Create data sets with KPIs from IN_DEC CDR
# MSIDN_index=0
# indec_data_path = "/user/hduser/sanjay/MP/MP/IN_Decrement_MP/DECREMENT_OUTPUT"+\
#                 "_2014*"
# indec_usage = sc.textFile(indec_data_path).coalesce(120)\
#                 .map(lambda x: x.split(','))\
#                 .filter(lambda x: x[1].split('-')[1] == month_filter)\
#                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                 msisdn_bc.value).map(lambda x: (x[MSIDN_index],float(x[2])))\
#                 .reduceByKey(add)  

# GROSS ARPU, NET ARPU, AON 

# ARPU_KPI_path = "/user/hduser/sanjay/KEL/vodafone_Monthly_ARPU/*"
# ARPU_col_list = []
# arpu = sc.textFile(ARPU_KPI_path)
# ARPU_KPI = read_data(d,max_col_index=1,delimiter='|',num_cols=3,cols_list=ARPU_col_list)\
#             .map(lambda x: (x[0],[x[1],x[2],x[3]]))

ARPU_KPI = sc.textFile(path_msisdn_info).map(lambda x:x.split(','))\
         .filter(lambda x:(int(x[2]) >=120) & (x[0] in fm_bc.value))\
         .map(lambda x:(x[0],[x[1],x[2]]))

# TOTAL BONUS RECHARGE, TOTAL RECHARGE, TOTAL BONUS RECHARGE COUNT, TOTAL RECHARGE COUNT  

rchg_KPI = read_data(d,max_col_index=2,delimiter=',',num_cols=3,cols_list=rchg_cols_list)\
            .map(lambda x:(num_check(x[0]),x[1].split(" ")[0],str(int(float(x[2])))))\
            .filter(lambda x: (x[1].split('-')[1] ==month_filter) & (x[0] in msisdn_bc.value))\
            .map(lambda x: (x[0],[float(x[2]) if x[2] in tt_rchg_val else 0.0,\
            float(x[2]) if x[2] not in tt_rchg_val else 0.0, \
            1 if x[2] in tt_rchg_val else 0,1 if x[2] not in tt_rchg_val else 0]))\
            .reduceByKey(add).map(lambda x: (x[0],map(str,x[1])))


# fm_rchg_data.leftOuterJoin(ogmou_data).map(lambda x: (x[0],x[1][0] + x[1][1]) if x[1][1] else (x[0],\
#         x[1][0]+['0','0','0','0','0','0'])).join(internet_usage).map(lambda x: (x[0],x[1][0] + \
#         [str(x[1][1])]) if x[1][1] else (x[0],x[1][0] + ['0'])).join(indec_usage).map(lambda x:\
#         (x[0],x[1][0] + [str(x[1][1])]) if x[1][1] else (x[0],x[1][0] + ['0']))\
#         .map(lambda x: x[0]+","+",".join(x[1])).saveAsTextFile(outpath+"/KL/KPIs")

fm_rchg_data.leftOuterJoin(ogmou_data).map(lambda x: (x[0],x[1][0] + x[1][1]) \
        if x[1][1] else (x[0],x[1][0]+['0','0','0','0','0','0'])).join(internet_usage)\
        .map(lambda x:(x[0],x[1][0] + [str(x[1][1])])if x[1][1] else (x[0],x[1][0] \
        + ['0'])).leftOuterJoin(rchg_KPI).map(lambda x: (x[0],x[1][0] + x[1][1]) \
        if x[1][1] else (x[0],x[1][0]+['0','0','0','0'])).leftOuterJoin(ARPU_KPI)\
        .map(lambda x: (x[0],x[1][0] + x[1][1]) if x[1][1] else (x[0],x[1][0]+\
        ['0','0'])).map(lambda x: x[0]+","+",".join(x[1]))\
        .saveAsTextFile(outpath+"/KL/KPIs_2")


