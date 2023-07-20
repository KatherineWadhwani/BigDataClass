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

def clean_text(text):
     text = text.lower()
     text = re.sub('\[.*?\]', '', text)
     text = re.sub('[%s]' % re.escape(string.punctuation), ' ', text)
     text = re.sub('[\d\n]', ' ', text)
     return text

import nltk
import nltk.corpus
from nltk.corpus import inaugural
nltk.download('inaugural')
for text in nltk.corpus.inaugural.fileids()[-10:] :
     corpus_view = inaugural.words(text)
     speech = ""
     for str in corpus_view:
          speech += clean_text(str)
     print(speech)










