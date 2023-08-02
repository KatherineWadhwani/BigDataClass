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

if __name__ == "__main__":
#Setup 
        #Open Poe Stories
	story = open("poe-stories/THE_BLACK_CAT", "r")


        #Convert Poe Stories to Lower
	for i in range(21):
		story = story.read()
		story = story.lower()

	#Tokenize story using Dr. J's code
	sent_text = nltk.sent_tokenize(story)     
	all_tagged = [nltk.pos_tag(nltk.word_tokenize(sent)) for sent in sent_text]
	
	adjs = [None] * 10
	nouns = [None] * 10
	verbs = [None] * 10
	adjCount = 0
	nounCount = 0
	verbCount = 0
	
	#Print word types
	for i in range(len(all_tagged)):
		for j in range(len(all_tagged[i])):
			tagType = all_tagged[i][j][1]
			if ((tagType == "JJ" or tagType == "JJR" or tagType == "JJS") and adjCount < 10):
	            		adjs[adjCount] = all_tagged[i][j][0]
	            		adjCount+=1
			if ((tagType == "NN" or tagType == "NNS" or tagType == "NNP" or tagType == "NNPS") and nounCount < 10):
	            		nouns[nounCount] = all_tagged[i][j][0]
	            		nounCount+=1
			if ((tagType == "VB" or tagType == "VBD" or tagType == "VBG" or tagType == "VBN" or tagType == "VBP" or tagType == "VBZ") and verbCount < 10):
	            		verbs[verbCount] = all_tagged[i][j][0]
	            		verbCount+=1

	print("adjectives (10/<total adjective count>):" + str(adjs))
	print("nouns (10/<total noun count>):" + str(nouns))
	print("verbs (10/<total verb count>):" +  str(verbs))


"""
	
	7.	JJ	Adjective
	8.	JJR	Adjective, comparative
	9.	JJS	Adjective, superlative

	12.	NN	Noun, singular or mass
	13.	NNS	Noun, plural
	14.	NNP	Proper noun, singular
	15.	NNPS	Proper noun, plural
	
	27.	VB	Verb, base form
	28.	VBD	Verb, past tense
	29.	VBG	Verb, gerund or present participle
	30.	VBN	Verb, past participle
	31.	VBP	Verb, non-3rd person singular present
	32.	VBZ	Verb, 3rd person singular present


"""

#---------------------------------------------------#


            
            
                        
