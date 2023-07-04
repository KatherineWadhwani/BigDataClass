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


text = f.read()
result = text.split("-")
print(result[0])
print ("HIYA THERE")




 
 
#df.show()
 
#df.printSchema()
