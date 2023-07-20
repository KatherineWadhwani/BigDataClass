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

presidents[10];
presidents[0] = inaugural.words('1981-Reagan.txt')
Reagan = inaugural.words('1989-Bush.txt')
ClintonOne = inaugural.words('1993-Clinton.txt')
ClintonTwo = inaugural.words('1997-Clinton.txt')
BushTwo = inaugural.words('2001-Bush.txt')
BushThree = inaugural.words('2005-Bush.txt')
ObamaOne = inaugural.words('2009-Obama.txt')
ObamaTwo = inaugural.words('2013-Obama.txt')
Idiot = inaugural.words('2017-Trump.txt')
Biden = inaugural.words('2021-Biden.txt')


import re
def clean_text(Reagan):
    Reagan = Reagan.lower()
    Reagan = re.sub('\[.*?\]', '', Reagan)
    Reagan = re.sub('[%s]' % re.escape(string.punctuation), ' ', Reagan)
    Reagan = re.sub('[\d\n]', ' ', Reagan)
    return Reagan








