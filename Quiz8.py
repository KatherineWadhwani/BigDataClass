#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import math
import string
import re
import pprint
import nltk
import csv
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession


#Setup 
f1 = pd.read_csv('ml-latest-small/movies.csv')
f2 = pd.read_csv('ml-latest-small/ratings.csv')
out = pd.merge(f1,f2,on='movieId',how='inner')
#print(out)
out.to_csv("merged.csv", index=False)


reviews = dict()

for num in range(331):
  list = [-1] * 10000
  reviews[num] = list



for row in out:
  list = reviews.get(row.userId)
  list.append(movieId)
  reviews[row.userId] = list

print(reviews)


