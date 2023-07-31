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

if __name__ == "__main__":
#Setup 
            #Open Poe Speaches
            files = [None] * 21
            files[0] = open("poe-stories/A_DESCENT_INTO_THE_MAELSTROM", "r")
            files[1] = open("poe-stories/BERENICE", "r")
            files[2] = open("poe-stories/ELEONORA", "r")
            files[3] = open("poe-stories/LANDORS_COTTAGE", "r")
            files[4] = open("poe-stories/MESMERIC_REVELATION", "r")
            files[5] = open("poe-stories/SILENCE_A_FABLE", "r")
            files[6] = open("poe-stories/THE_ASSIGNATION", "r")
            files[7] = open("poe-stories/THE_BLACK_CAT", "r")
            files[8] = open("poe-stories/THE_CASK_OF_AMONTILLADO", "r")
            files[9] = open("poe-stories/THE_DOMAIN_OF_ARNHEIM", "r")
            files[10] = open("poe-stories/THE_FACTS_IN_THE_CASE_OF_M_VALDEMAR", "r")
            files[11] = open("poe-stories/THE_FALL_OF_THE_HOUSE_OF_USHER", "r")
            files[12] = open("poe-stories/THE_IMP_OF_THE_PERVERSE", "r")
            files[13] = open("poe-stories/THE_ISLAND_OF_THE_FAY", "r")
            files[14] = open("poe-stories/THE_MASQUE_OF_THE_RED_DEATH", "r")
            files[15] = open("poe-stories/THE_PIT_AND_THE_PENDULUM", "r")
            files[16] = open("poe-stories/THE_PREMATURE_BURIAL", "r")
            files[17] = open("poe-stories/THE_PURLOINED_LETTER", "r")
            files[18] = open("poe-stories/THE_THOUSAND_AND_SECOND_TALE_OF_SCHEHERAZADE", "r")
            files[19] = open("poe-stories/VON_KEMPELEN_AND_HIS_DISCOVERY", "r")
            files[20] = open("poe-stories/WILLIAM_WILSON", "r")

            #Clean Poe Speaches
            data = [None] * 21
            for i in range(21):
                        data[i] = files[i].read()
                        data[i] = re.sub('[^0-9a-zA-Z]+', ' ', data[i])
                        data[i] = data[i].lower()

            #Tokenize first story using Dr. J's code
            sent_text = nltk.sent_tokenize(data[1])     
            all_tagged = [nltk.pos_tag(nltk.word_tokenize(sent)) for sent in sent_text]

            #Print dict
            tagdict = load('help/tagsets/upenn_tagset.pickle')
            df = {
              "text": data[1],
              "dictionary": tagdict
            }
            print(df);

            #Print word in file
            reviewsDict = []

            with open('reviews.csv', newline='') as csvfile:
                        reader = csv.reader(csvfile, delimiter=' ')
                        for row in reader:
                                    reviewsDict.append(row)

            reviewsDF = pd.DataFrame(reviewsDict, columns=['Filename', 'Speech'])
            def sent_to_words(sentences):
                        for sentence in sentences:
                                    yield(gensim.utils.simple_preprocess(str(sentence), deacc=True))

            
