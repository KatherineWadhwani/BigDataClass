#!/usr/bin/env python3
import pandas as pd
import os
import math
import string
import numpy as np
import re
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from nltk.corpus.reader.util import StreamBackedCorpusView

corpusDict = {}

dictOne = {}
dictTwo = {}
dictThree = {}
dictFour = {}
dictFive = {}
dictSix = {}
dictSeven = {}
dictEight = {}
dictNine = {}
dictTen = {}

dictArray = [dictOne, dictTwo, dictThree, dictFour, dictFive, dictSix, dictSeven, dictEight, dictNine, dictTen]

highestOne = [None]*20
highestTwo = [None]*20
highestThree = [None]*20
highestFour = [None]*20
highestFive = [None]*20
highestSix = [None]*20
highestSeven = [None]*20
highestEight = [None]*20
highestNine = [None]*20
highestTen = [None]*20

highest = [highestOne, highestTwo, highestThree, highestFour, highestFive, highestSix, highestSeven, highestEight, highestNine, highestTen]

def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('[\d\n]', ' ', text)
    text = re.sub(r'[^a-z]', '', text)
    return text

def calculateIDF(term):
    count = 0
    for x in range(10):
        if term in dictArray[x]:
            count+=1
    return math.log2(10/count)

def calculateTFIDF():
    for x in range(10):
         for term in dictArray[x]:
            dictArray[x][term] = dictArray[x].get(term)/len(dictArray[x])
            dictArray[x][term] = dictArray[x].get(term)*calculateIDF(term)

def getHighest(num):
   print(max(zip(dictArray[x].values(), dictArray[x].keys())))
    
import nltk
import nltk.corpus
from nltk.corpus import inaugural
nltk.download('inaugural')
x = 0
for text in nltk.corpus.inaugural.fileids()[-10:] :
    array = inaugural.words(text)
    for str in array:
        word = clean_text(str)
        if word not in corpusDict.keys() and word != '':
            corpusDict.update({word: 1})
        elif word != '':
            value = corpusDict.get(word)
            value += 1
            corpusDict[word] = value
        if word not in dictArray[x].keys() and word != '':
            dictArray[x].update({word: 1})
        elif word != '':
            value = dictArray[x].get(word)
            value += 1
            dictArray[x][word] = value

    x+=1
    
calculateTFIDF()
getHighest(6)


#IDF(t,D) $= log[($size of D$)/($size of D that contain t$)]$, where t is a given word and D is the corpus of documents.









