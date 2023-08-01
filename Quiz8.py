#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import math
import string
import re
import pprint
import matplotlib.pyplot as plt
import seaborn as sns
import nltk
import csv
import pyLDAvis
import pyLDAvis.gensim 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from nltk.corpus.reader.util import StreamBackedCorpusView
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.data import load
from nltk.corpus import stopwords
nltk.download('punkt')
nltk.download('tagsets')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')
import gensim
from gensim import corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "spark-3.2.1-bin-hadoop3.2"
import spacy
spacy.load('en_core_web_sm')
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)
import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)
stop_words = stopwords.words('english')


#Setup 

all_files = glob.glob(os.path.join("ml-latest-small/", "*.csv"))

df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
print(df)
