#!/usr/bin/env python3
import time, datetime, sys
from datetime import datetime, date
import pandas as ps
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

response[]
requestType[]
ipAddress[]

for row in rows:
    data = re.split(' - - | "| /|" ', rows[counter])
    response[counter] = data[7]
    requestType[counter] = data[5]
    ipAddress[counter] = data[0]
    counter+=1

print("down")
print(ipAddress)
print("uppity")


 
#df.show()
 
#df.printSchema()
