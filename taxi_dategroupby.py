
# coding: utf-8

## uses pyspark to aggregate NYC taxi data for 2013 and bucket the trips by hour (count of trips per hour)

import re
from pyspark.sql import SQLContext, Row
import datetime
import pandas as pd
import csv
from pyspark import SparkContext
import re

# set spark context
sc = SparkContext()

# pass a list of file extensions for the csv files here
# e.g. contents = [['file1.csv'],['file2.csv'],['file3.csv']]
contents = [] 



# iterate over each .csv file and union them into a single RDD called *tx*
file_iter = 0

for file in contents:
    item = sc.textFile("hdfs namenode" + file[0]) #provide the hdfs namenode url
    
    if file_iter != 0:
        tx = tx.union(item)
    else:
        tx = item
    file_iter += 1


# define a function to convert dates within the RDD to a machine readable format
def convertdate (item):
    date = datetime.datetime.strptime(item, '%Y-%m-%d %H:%M:%S')
    return date


# define a rounding function to extract the hour containing the given ride
def round_to_hour(item):
    item = str(item)
    item_parsed = item[0:4] + item[5:7] + item[8:10] + item[11:13]
    if (re.search('[0-9]{10}', item_parsed)):
        rounded_item = pd.to_datetime(item_parsed, format='%Y%m%d%H')
    else:
        # note that 12 of 170 million failed to parse
        rounded_item = 'date_fail'
    return rounded_item


# lambda function that maps the rounding function to each item within the RDD
# returns a tuple in the form of (rounded date, 1)
dropoff_times = tx.map(lambda (x): (round_to_hour(x.split(',')[5]),1))


# reduceby with an add function addes up the 2nd item in the tuples against the date key
from operator import add
time_counts = dropoff_times.reduceByKey(add)


# collects the output
counts = time_counts.collect()

# converts datetime back to string
counts = [(str(time),count) for time,count in counts]

# writes a csv as output to Spark driver
with open("counts.csv",'w') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['date','count'])
    for row in counts:
        csv_out.writerow(row)

