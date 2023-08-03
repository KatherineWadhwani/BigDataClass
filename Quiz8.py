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
def hash(hashNum):
  for num in range(611):
    if(len(reviewsJaccard[num]) > 0):
      M = 2^13 - 1
      movie = f1.loc[((5*hashNum + 1) % M, "movieCount")]
      if(movie in reviewsJaccard[num]):
        list = reviewsMinHash[num]
        list.append(movie)
        reviewsMinHash[num] = list
        break

def computeJacc(num1, num2):
  set1 = set(reviewsJaccard[num1])
  set2 = set(reviewsJaccard[num2])
  intersect = len(set1.intersection(set2))
  union = len(set1.union(set2))
  jaccard = (intersect/union)
  if (jaccard > 0.5):
    print("Users " + str(num1) + " and " + str(num2) + " have a Jaccard similarity of " + str(jaccard))

def computeMinHash(num1, num2):
  list1 = reviewsMinHash[num1]
  list2 = reviewsMinHash[num2]
  intersect = 0
  union = len(list1) + list(list2) - union
  for i in range(193609):
    if(list1[i] == list2[i]):
      intersect += 1
  print(similarity/union)
      
  union = len(set1.union(set2))
  jaccard = (intersect/union)
  if (jaccard > 0.5):
    print("Users " + str(num1) + " and " + str(num2) + " have a Jaccard similarity of " + str(jaccard))

f1 = pd.read_csv('ml-latest-small/movies.csv')
f1['movieCount'] = range(9742)
f2 = pd.read_csv('ml-latest-small/ratings.csv')
out = pd.merge(f1,f2,on='movieId',how='inner')
out.to_csv("merged.csv", index=False)
del out['timestamp']
del out['rating']

reviewsJaccard = dict()
reviewsMinHash = dict()

for num in range(611):
  list = []
  listTwo = []
  reviewsJaccard[num] = list
  reviewsMinHash[num] = listTwo

#Assign movie to user who reviewd it
for i in range(len(out)):
  out.loc[i, "movieId"] =  out.loc[i, "movieCount"]
  list = reviewsJaccard.get(out.loc[i, "userId"])
  list.append(out.loc[i, "movieId"])
  reviewsJaccard[out.loc[i, "userId"]] = list
  

"""for num1 in range(611):
  for num2 in range(611):
    if (num1 != num2 and num1 < num2):
      computeJacc(num1, num2)"""

for num in range(1, 51):
  hash(num)





