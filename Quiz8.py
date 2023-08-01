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

#does order matter or can use set?
#Setup 
def hash1(num):
  M = 2^13 - 1
  print ((num*50 + 1) % M)

def hash2(num):
  M = 2^13 - 1
  print ((num*100 + 1) % M)

def hash3(num1):
  M = 2^13 - 1
  print ((num*200 + 1) % M)
  

def computeJacc(num1, num2):
  list1 = reviews[num1]
  list2 = reviews[num2]
  intersect = len(list1.intersection(list2))
  union = len(list1.union(list2))
  jaccard = (intersect/union)
  if (jaccard > 0.5):
    print("Users " + str(num1) + " and " + str(num2) + " have a Jaccard similarity of " + str(jaccard))

f1 = pd.read_csv('ml-latest-small/movies.csv')
f2 = pd.read_csv('ml-latest-small/ratings.csv')
out = pd.merge(f1,f2,on='movieId',how='inner')
#print(out)
out.to_csv("merged.csv", index=False)

reviews = dict()

for num in range(611):
  list = set()
  reviews[num] = list

for i in range(len(out)):
  list = reviews.get(out.loc[i, "userId"])
  list.add(out.loc[i, "movieId"])
  
for num1 in range(611):
  for num2 in range(611):
    if (num1 != num2):
      computeJacc(num1, num2)
      hash1(num1)





