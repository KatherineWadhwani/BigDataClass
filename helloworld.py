#!/usr/bin/env python3
import time, datetime, sys
from datetime import datetime, date
import pandas as pd
import os
import re
import math
import numpy as np
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row


spark = SparkContext.getOrCreate()

spark.addPyFile(SparkFiles.get("/home/kat_wadhwani/BigDataClass/access.log"))


f = open("/home/kat_wadhwani/BigDataClass/access.log", "r")


text = f.read()
rows = text.splitlines()

rows.pop(0)
counter = 0
entries = len(rows)

response = [None] * entries
requestType = [None] * entries
ipAddress = [None] * entries

for row in rows:
    data = re.split(' - - | "| /|" ', rows[counter])
    response[counter] = data[4]
    requestType[counter] = data[2]
    ipAddress[counter] = data[0]
    counter+=1

counter = 0
for row in response:
    data = row.split(" ")
    response[counter] = data[0]
    counter+=1


percentile_list = pd.DataFrame(list(zip(ipAddress, requestType, response)),
              columns=['ipAddress','requestType', 'response'])
 
 
percentile_list.printSchema()
