#!/usr/bin/env python3
import time, datetime, sys
from datetime import datetime, date
import pandas as ps
import os
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row


spark = SparkContext.getOrCreate()

spark.addPyFile(SparkFiles.get("/home/kat_wadhwani/BigDataClass/access.log"))


f = open("/home/kat_wadhwani/BigDataClass/access.log", "r")


with f as input:
   for line in input:
      if '\"_\"' in line:
         break
         print(line)




 
 
#df.show()
 
#df.printSchema()
