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

sqlContext.createDataFrame([
    Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1),
        e=datetime(2000, 8, 1, 12, 0)),
   
    Row(a=2, b=8., c='GFG2', d=date(2000, 6, 2),
        e=datetime(2000, 6, 2, 12, 0)),
   
    Row(a=4, b=5., c='GFG3', d=date(2000, 5, 3),
        e=datetime(2000, 5, 3, 12, 0))
])
 
 
sqlContext.show()
 
sqlContext.printSchema()
