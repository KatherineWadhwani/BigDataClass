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


import nltk
import nltk.corpus
from nltk.corpus import inaugural
nltk.download('inaugural')
nltk.corpus.inaugural.fileids()[-10:]


session = SparkSession \
    .builder \
    .appName("data_import") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark = SparkContext.getOrCreate()
sqlContext = SQLContext(spark)
spark.addPyFile(SparkFiles.get("/home/kat_wadhwani/nltk_data/corpora/inaugural"))





