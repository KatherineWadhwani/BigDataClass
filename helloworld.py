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

import re



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

presidents = [10]
presidents[0] = inaugural.words('1981-Reagan.txt')
presidents[1] = inaugural.words('1989-Bush.txt')
presidents[2] = inaugural.words('1993-Clinton.txt')
presidents[3] = inaugural.words('1997-Clinton.txt')
presidents[4] = inaugural.words('2001-Bush.txt')
presidents[5] = inaugural.words('2005-Bush.txt')
presidents[6] = inaugural.words('2009-Obama.txt')
presidents[7] = inaugural.words('2013-Obama.txt')
presidents[8] = inaugural.words('2017-Trump.txt')
presidents[9] = inaugural.words('2021-Biden.txt')











