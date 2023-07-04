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



sqlContext = SQLContext(spark)

df = sqlContext.createDataFrame([
  row_rdd = spark.map(row_parse_function).filter("_")
])
 
 
df.show()
 
df.printSchema()
