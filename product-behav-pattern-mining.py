#!/usr/bin/env python
"""product-behav-pattern-mining.py: Identifies important patterns in usage 
   behavior and margin."""

__author__     = "Sanjay Kumar"
__copyright__  = "Flytxt Mobile Solutions"
__license__    = "GPL"
__version__    = "0.0.1"
__email__      = "sanjay.kumar@flytxt.com"
__status__     = "Experimental"

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

# Identify set of subscribers who have made recharges from 27th Nov to 3rd of 
# Dec but no recharges X days prior to and after it. Choice of X = 30 as most
# products have a typical validity of 30 days. The filter criteria to ignore
# all of subscribers who have recharged in between the pre and post X day 
# periods is because measuring the margin impact of the products becomes fairly
# complicated otherwise. 

# Identify associated prepost behaviors of subscribers mentioned from previous 
# step. 

# Mine frequent patterns and association rules using FP growth algorithm to 
# identify recurring patterns

