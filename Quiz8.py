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
def hashOne(num):
  M = 2^13 - 1
  list = reviewsMinHash[num]
  for index in range(len(list)):
    list[index] = ((list[index]*50 + 1) % M)
  reviewsMinHash[num] = list
  #print(reviewsMinHash[num])

def hashTwo(num):
  M = 2^13 - 1
  list = reviewsMinHash[num]
  for index in range(len(list)):
    if (list[index] > (list[index]*100 + 1) % M):
      list[index] = ((list[index]*100 + 1) % M)
  reviewsMinHash[num] = list
  #print(reviewsMinHash[num])
    
def hashThree(num):
  M = 2^13 - 1
  list = reviewsMinHash[num]
  for index in range(len(list)):
    if (list[index] > (list[index]*200 + 1) % M):
      list[index] = ((list[index]*200 + 1) % M)
  reviewsMinHash[num] = list
  #print(reviewsMinHash[num])
  
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
f2 = pd.read_csv('ml-latest-small/ratings.csv')
out = pd.merge(f1,f2,on='movieId',how='inner')
#print(out)
out.to_csv("merged.csv", index=False)

reviewsJaccard = dict()
reviewsMinHash = dict()

for num in range(611):
  list = []
  reviewsJaccard[num] = list
  reviewsMinHash[num] = [0] * 193609

for i in range(len(out)):
  list = reviewsJaccard.get(out.loc[i, "userId"])
  list.append(out.loc[i, "movieId"])

for num in range(611):
  for i in range(193609):
    if (i in reviewsJaccard[num]):
      reviewsMinHash[num][i] = 1
  hashOne(num)
  hashTwo(num)
  hashThree(num)

for num1 in range(611):
  for num2 in range(611):
    if (num1 != num2):
      computeJacc(num1, num2)
      computeMinHash(num1, num2)





