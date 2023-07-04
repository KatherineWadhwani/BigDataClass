#!/usr/bin/env python3
import time, datetime, sys
from datetime import datetime, date
import pandas as ps
import os
import re 
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row


spark = SparkContext.getOrCreate()

spark.addPyFile(SparkFiles.get("/home/kat_wadhwani/BigDataClass/access.log"))


f = open("/home/kat_wadhwani/BigDataClass/access.log", "r")


text = f.read()
rows = text.split("\"-\"")
print(rows[0])

str1 = "- -"
str2 = "\""
str3 = " /"
print(re.split(str1, str2, str3, rows[0]))

 
 
#df.show()
 
#df.printSchema()
