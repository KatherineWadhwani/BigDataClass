#!/usr/bin/env python3
import time, datetime, sys
import pandas as pd
import os
from pyspark import SparkContext
from pyspark import SparkFiles

spark = SparkContext.getOrCreate()

file f = spark.addPyFile(SparkFiles.get("/home/kat_wadhwani/BigDataClass/access.log"))

ps.read_csv()
