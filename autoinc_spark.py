#!/usr/bin/env python

from pyspark.sql import *
from pyspark import SparkContext

def extract_vin_key_value(line: str):
    values = line.split(",") # reads in data from data.csv
    type = values[1]
    vin_num = values[2]
    make = values[3]
    year = values[5]
    PairRDD = (make, year, type)
    return (vin_num, PairRDD)


sc = SparkContext("local", "My Application")
raw_rdd = sc.textFile("data.csv")

vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

enhance_make = vin_kv.groupByKey()\
                     .flatMap(lambda kv: kv[1])\
                     .filter(lambda x: len(x[1]) > 0 and len(x[2]) > 0)

make_kv = enhance_make.map(lambda x: x[0] + '-' + x[1])

make_kv_count = make_kv.map(lambda x: (x, 1))\
                       .reduceByKey(lambda x, y: x+y)

print(*make_kv_count.collect(), sep = '\n')