#!/usr/bin/env python3
import pandas as pd
import os
import math
import string
import numpy as np
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from nltk.corpus.reader.util import StreamBackedCorpusView
import sys
import datetime

sc = SparkContext(appName="Proj7")
ssc = StreamingContext(sc, 2)

lines =  ssc.socketTextSTream("localhost", 9999)


googPrice = lines.flatMap(lambda line: line.split(" "))\
            .map(lambda word: (word, 1))\
            .reduceByKey(lambda a, b: a + b).take(2)

googPrice.print()

ssc.start()
ssc.awaitTermination()




