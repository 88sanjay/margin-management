
#!/usr/bin/env python
"""product-patttern-dataset-creation.py: Creation of data set for analysis 
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
    elif date_format == "YYYY-MM-DD":
        parsed_date_str = date_str.split(' ')[0]
        delta = (date(2014,int(parsed_date_str.split('-')[1]),int(parsed_date_str\
            .split('-')[2])) - date(2014,11,1)).days
    else:
        delta = -999
    return int(delta/7)

def map_to_array(idx,val,length=7):
    s = [0 for i in range(0,length)]
    s[idx+int(length/2)] = val # to correct for negative values of week passed
    return s

#Function to create RDD of behavioral KPIs
def aggregate_week_month(rdd,MSISDN_idx,KPI_idx,date_idx,date_format,outname):
    return rdd.map(lambda x:(x[MSISDN_idx],get_week(x[date_idx],date_format),\
        (float(x[KPI_idx]) if x[KPI_idx] else 0.0))).filter(lambda x: \
        ((x[1] < 4) & (x[1] > -4))).map(lambda x: (x[0],map_to_array(x[1],x[2])\
        )).reduceByKey(add).map(lambda x: x[0]+","+",".join(map(str,x[1])))\
        .saveAsTextFile(outname)

def make_rchg_unq(x):
    rchg = x[1:]
    rchg.sort()
    prev = '0'
    for i in range(0,len(rchg)) :
        count = (count+1) if (prev == rchg[i]) else 1
        prev = rchg[i]
        rchg[i] = rchg[i]+"_"+str(count)
    return(",".join(rchg))


#sc = SparkContext("spark://master:7077",appName="product-behav-pattern-mining")

# Identify recharge combos for a month
# Initial cleaning of inputs to get rdds in required format for parsing. At the
# end of this step we should have required rdds for further processing

#This path contains files of <MSISDN, ARPU, AON> tuples
# path_msisdn_info = "/user/hduser/sanjay/MP/Output/MSISDN_MASTER_FILE/Month_1/*"
path_msisdn_info = "/user/hduser/sanjay/KEL/MSISDN_MASTER/*"

#This path contains recharge event files
path_rchg_info   = "/user/hduser/sanjay/KEL/vodafone_recharges/KE_Recharge_event_*"
#This path is for storing datasets
outpath = "/user/hduser/sanjay/margin-mgmt/"

month_filter = '11'
# tt_rchg_val = ['6','7','5','10','20','21','30','42','47','50','60','67','70','80','81','87','100','111','200','222','225','250',\
#                 '270','333','400','500','750','1000','2000','3000']
tt_rchg_val = ['5000','4000','3500','3000','2000','1001','1000','900','800','786','750','700'\
                ,'600','501','551','500','400','351','350','330','270','250','220','125','100',\
                '65','50','30','20','10']
d = sc.textFile(path_rchg_info).coalesce(100)
rchg_data = read_data(d,max_col_index=2,delimiter=',',num_cols=3, #cols_list=[0,2,3])
            cols_list=[0,1,2])\
            .map(lambda x:(num_check(x[0]),x[1].split(" ")[0],str(int(float(x[2])))))\
            .filter(lambda x: (x[1].split('-')[1] ==month_filter) & (x[2] not in tt_rchg_val))

filtered_msisdns =  rchg_data.map(lambda x: (x[0],0)).distinct()

fm_bc = sc.broadcast(filtered_msisdns.collectAsMap())
msisdn = sc.textFile(path_msisdn_info).map(lambda x:x.split(','))\
         .filter(lambda x:(int(x[2]) >=120) & (x[0] in fm_bc.value))\
         .map(lambda x:(x[0],0))

msisdn_bc = sc.broadcast(msisdn.collectAsMap())
fm_bc.unpersist()
fm_rchg_data = rchg_data.filter(lambda x: x[0] in msisdn_bc.value).map(lambda x:\
                (x[0],x[2])).reduceByKey(lambda a,b: a+","+b).map(lambda x: x[0]+","+x[1])

# fm_rchg_data.saveAsTextFile(outpath+"fm_rchg_data")
# Making each reacharge unique
# fm_rchg_data_unq = fm_rchg_data.map(lambda x: make_rchg_unq(x.split(',')))\
#                     .saveAsTextFile(outpath+"fm_rchg_data_unq_1mth_v2")

fm_rchg_data.map(lambda x: [i for i in set(x.split(',')[1:])])\
                    .map(lambda x: ",".join(x))\
                    .saveAsTextFile(outpath+"fm_rchg_non_tt_unq_kel_nov")

# # Create data sets with other KPIs from OG MOU CDR 
# MSIDN_index=0
# og_data_path = '/user/hduser/sanjay/MP/MP/og_voice_usage/'
# ogmou_data = sc.textFile(og_data_path).coalesce(120).map(lambda x: x.split(','))\
#             .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])] + \
#             x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#             msisdn_bc.value).persist(StorageLevel.MEMORY_AND_DISK)

# local_og_data  = aggregate_week_month(ogmou_data,0,23,1,'MM-DD-YY',(outpath+\
#                 "local_og_data"))
# std_og_data = aggregate_week_month(ogmou_data,0,4,1,'MM-DD-YY',(outpath+\
#                 "std_og_data"))
# onnet_og_data = aggregate_week_month(ogmou_data,0,3,1,'MM-DD-YY',(outpath+\
#                 "onnet_og_data"))
# isd_og_data =  aggregate_week_month(ogmou_data,0,19,1,'MM-DD-YY',(outpath+\
#                 "isd_og_data"))
# total_og_data = aggregate_week_month(ogmou_data,0,13,1,'MM-DD-YY',(outpath+\
#                 "total_og_data"))
# sms_data = aggregate_week_month(ogmou_data,0,2,1,'MM-DD-YY',(outpath+"sms_data"))

# ogmou_data.unpersist()

# # Create data sets with other KPIs from Data CDR
# MSIDN_index=0
# internet_data_path = "/user/hduser/sanjay/MP/MP/data_trigger/MP_Data_usage_"+\
#                     "trigger_2014*"
# internet_usage = sc.textFile(internet_data_path).coalesce(120)\
#                 .map(lambda x: x.split(','))\
#                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                 msisdn_bc.value).persist(StorageLevel.MEMORY_AND_DISK)  
# internet_data  = aggregate_week_month(internet_usage,0,3,2,'YYYY-MM-DD',(outpath+\
#                 "internet_data"))
# internet_usage.unpersist()

# # Create data sets with KPIs from IN_DEC CDR
# MSIDN_index=0
# indec_data_path = "/user/hduser/sanjay/MP/MP/IN_Decrement_MP/DECREMENT_OUTPUT"+\
#                 "_2014*"
# indec_usage = sc.textFile(indec_data_path).coalesce(120)\
#                 .map(lambda x: x.split(','))\
#                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                 msisdn_bc.value).persist(StorageLevel.MEMORY_AND_DISK)  
# indec_data  = aggregate_week_month(indec_usage,0,2,1,'YYYY-MM-DD',(outpath+\
#                 "indec_data"))
# indec_usage.unpersist()

# # Create data sets with IC CDR
# MSIDN_index=2
# path_to_month_files = "/user/hduser/sanjay/MP/Monthdata/"
# pre_profile_path = path_to_month_files+"month_flytxt_usage_20141106_87_6239595"+\
#                     "69.dat"
# post_profile_path = path_to_month_files+"month_flytxt_usage_20141206_87_643015"+\
#                     "471.dat"

# pre_profile_data = sc.textFile(pre_profile_path).coalesce(120)\
#                 .map(lambda x: x.split('|'))\
#                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                 msisdn_bc.value).map(lambda x:",".join(map(str,[x[MSIDN_index], \
#                 x[3],x[4],x[8]]))).saveAsTextFile(outpath+"pre_profile_data")  

# post_profile_data = sc.textFile(post_profile_path).coalesce(120)\
#                 .map(lambda x: x.split('|'))\
#                 .map(lambda x:x[0:MSIDN_index] + [num_check(x[MSIDN_index])]+\
#                 x[MSIDN_index+1:]).filter(lambda x: x[MSIDN_index] in \
#                 msisdn_bc.value).map(lambda x:",".join(map(str,[x[MSIDN_index], \
#                 x[3],x[4],x[8]]))).saveAsTextFile(outpath+"post_profile_data")  


# d.map(lambda x:(x[0],int(x[2]))).reduceByKey(append).filter(lambda x: 0 in x[1]).count()

