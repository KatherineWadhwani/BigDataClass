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

def hash(hashNum):
  for num in range(611):
      M = 2**13 - 1
      for integer in range(1, 9743):
        movie = f1.loc[((integer*hashNum + 1) % M, "movieCount")]
        if(movie in reviewsJaccard[num]):
          list = reviewsMinHash[num]
          list.append(movie)
          reviewsMinHash[num] = list
          print(list)
          break

def computeJacc(num1, num2):
  set1 = set(reviewsJaccard[num1])
  set2 = set(reviewsJaccard[num2])
  intersect = len(set1.intersection(set2))
  union = len(set1.union(set2))
  jaccard = (intersect/union)
  if (jaccard > 0.5):
    pairsJaccard.append([num1, num2])
    print("Users " + str(num1) + " and " + str(num2) + " have a Jaccard similarity of " + str(jaccard))

def computeMinHash(num1, num2):
  set1 = set(reviewsMinHash[num1])
  set2 = set(reviewsMinHash[num2])
  intersect = len(set1.intersection(set2))
  union = len(set1.union(set2))
  minHash = (intersect/union)
  if (minHash > 0.5):
    pairsMinHash.append([num1, num2])
    print("Users " + str(num1) + " and " + str(num2) + " have a minHash similarity of " + str(minHash))

def compare():
  truePos = 0
  falsePos = 0
  falseNeg = 0
  
  for int in range(len(pairsMinHash)):
    if (pairsMinHash[int] in pairsJaccard)
      truePos+=1
    if (pairsMinHash[int] not in pairsJaccard)
      falsePos+=1
  print("True Positive count is: " + str(truePos) + " and False Positive count is: " + str(falsePos))
  
  for int in range(len(pairsJaccard)):
    if (pairsJaccard[int] not in pairsMinHash)
      falseNeg+=1
  print("False Negative count is: " + str(falseNeg))

  totalPossiblePairs = (610 * 609)/2
  trueNeg = totalPossiblePairs - truePos - falsePos - falseNeg
  
  print("True Negative count is: " + str(trueNeg))
  

f1 = pd.read_csv('ml-latest-small/movies.csv')
f1['movieCount'] = range(9742)
f2 = pd.read_csv('ml-latest-small/ratings.csv')
out = pd.merge(f1,f2,on='movieId',how='inner')
out.to_csv("merged.csv", index=False)
del out['timestamp']
del out['rating']

reviewsJaccard = dict()
reviewsMinHash = dict()
pairsJaccard = []
pairsMinHash = []

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

  
for num in range(1, 51):
  hash(num)


for num in range(51, 101):
  hash(num)
for num in range(101, 201):
  hash(num)

for num1 in range(611):
  for num2 in range(611):
    if (num1 != num2 and num1 < num2):
      computeJacc(num1, num2)
      computeMinHash(num1, num2)

compare()








