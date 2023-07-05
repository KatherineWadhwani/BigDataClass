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

array = []
for row in rows:
    array[count] = re.split(' - - | "| /|" ', rows[counter])
    counter+=1

print(array[0]);



 
#df.show()
 
#df.printSchema()
