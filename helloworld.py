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
from nltk.corpus.reader.util import StreamBackedCorpusView

import re

def clean_text(text):
     text = text.lower()
     text = re.sub('\[.*?\]', '', text)
     text = re.sub('[%s]' % re.escape(str.punctuation), ' ', text)
     text = re.sub('[\d\n]', ' ', text)
     return text

import nltk
import nltk.corpus
from nltk.corpus import inaugural
nltk.download('inaugural')
for text in nltk.corpus.inaugural.fileids()[-10:] :
     corpus_view = inaugural.words(text)
     str_list = [str(w) for w in corpus_view]
     clean_text(str_list[0])










