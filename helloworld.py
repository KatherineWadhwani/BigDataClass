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
from pyspark.sql import SparkSession



session = SparkSession \
    .builder \
    .appName("data_import") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark = SparkContext.getOrCreate()
sqlContext = SQLContext(spark)

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


pandasDF = pd.DataFrame(list(zip(ipAddress, requestType, response)),
              columns=['ipAddress','requestType', 'response'])


sparkDF = sqlContext.createDataFrame(pandasDF)
#sparkDF.show()
#sparkDF.printSchema()

#sparkDF.write.saveAsTable("sample")


#errors

df_errors = sqlContext.sql("""SELECT ipAddress FROM sample WHERE CAST(response as INT) > 399""")

#df_errors.show()
#df_errors.printSchema()


#responseType
df_100s = sqlContext.sql("""SELECT * FROM sample WHERE CAST(response as INT) BETWEEN  100 AND 199 """)
df_200s = sqlContext.sql("""SELECT * FROM sample WHERE CAST(response as INT) BETWEEN 200 AND 299 """)
df_300s = sqlContext.sql("""SELECT * FROM sample WHERE CAST(response as INT) BETWEEN 300 AND 399 """)
df_400s = sqlContext.sql("""SELECT * FROM sample WHERE CAST(response as INT) BETWEEN 400 AND 499 """)
df_500s = sqlContext.sql("""SELECT * FROM sample WHERE CAST(response as INT) BETWEEN 500 AND 599 """)

OneHundredRows = df_100s.count()
TwoHundredRows = df_200s.count()
ThreeHundredRows = df_300s.count()
FourHundredRows = df_400s.count()
FiveHundredRows = df_500s.count()


print(f"The percentage of 100s is : {OneHundredRows* 100/entries}")
print(f"The percentage of 200s is : {TwoHundredRows* 100/entries}")
print(f"The percentage of 300s is : {ThreeHundredRows* 100/entries}")
print(f"The percentage of 400s is : {FourHundredRows* 100/entries}")
print(f"The percentage of 500s is : {FiveHundredRows* 100/entries}")

#requestType
df_GET = sqlContext.sql("""SELECT * FROM sample WHERE requestType ='GET'""")
df_PUT= sqlContext.sql("""SELECT * FROM sample WHERE requestType ='PUT'""")
df_POST = sqlContext.sql("""SELECT * FROM sample WHERE requestType ='POST'""")
df_DELETE = sqlContext.sql("""SELECT * FROM sample WHERE requestType ='DELETE'""")

getRows = df_GET.count()
putRows = df_PUT.count()
postRows = df_POST.count()
deleteRows = df_DELETE.count()

print(f"The percentage of GET requests is : {getRows* 100/entries}")
print(f"The percentage of PUT requests is : {putRows* 100/entries}")
print(f"The percentage of POST requests is : {postRows* 100/entries}")
print(f"The percentage of DELETE requests is : {deleteRows* 100/entries}")



