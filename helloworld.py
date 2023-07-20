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

def clean_text(text):
    text = text.lower()
    text = re.sub('\[.*?\]', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('[\d\n]', ' ', text)
    return text

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
        print(str)










