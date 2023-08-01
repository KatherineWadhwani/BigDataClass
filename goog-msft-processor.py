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
	
            adjs = []
            nouns = []	
            verbs = []
	
            #Print dict
            tagdict = load('help/tagsets/upenn_tagset.pickle')
            for i in range(len(all_tagged[0])):
            	tagType = all_tagged[0][i][1]
            	adjCount = 0
            	nounCount = 0
            	verbCount = 0
            	if (tagType == "JJ" or tagType == "JJR" or tagType == "JJS"):
            		adjs[adjCount] = all_tagged[0][i][0]
            		adjCount++
            	if (tagType == "NN" or tagType == "NNS" or tagType == "NNP" or tagType == "NNPS"):
			nouns[nounCount] = all_tagged[0][i][0]
			nounCount++
            	if (tagType == "VB" or tagType == "VBD" or tagType == "VBG" or tagType == "VBN" or tagType == "VBP" or tagType == "VBZ"):
			verbs[verbCount] = all_tagged[0][i][0]
			verbCount++

            print(adjs)
            print(nouns)
            print(verbs)


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

            #Print word in file
            reviewsDict = []
            
            """def collect (sentences):
                        speech = []
                        for sent in sentences:
                                for fragment in sent:
                                    speech.extend(fragment)
                        return speech

            def clean_sents(data):
                        # Remove new line characters
                        data = re.sub('\s+', ' ', str(data))
                        data = re.sub('[(.*!@#$%^&*\'";:/?,~`+=|)]', '', str(data))
                        data = data.lower()
                        return data
                        
            def sent_to_words(sentence):
                        words = sentence.split(" ")
                        for word in words:
                                    yield(gensim.utils.simple_preprocess(str(word).encode('utf-8'), deacc=True))  # deacc=True removes punctuations

            def remove_stopwords(texts):
                return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]
            
            def make_bigrams(texts):
                return [bigram_mod[doc] for doc in texts]
            
            def make_trigrams(texts):
                return [trigram_mod[bigram_mod[doc]] for doc in texts]
            
            def lemmatization(texts, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV']):
                #https://spacy.io/api/annotation
                texts_out = []
                for sent in texts:
                    doc = nlp(" ".join(sent)) 
                    texts_out.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])
                return texts_out
                                               

            colnames = ['recNo', 'ClothingID', 'Age', 'Title', 'ReviewText', 'Rating', 'ReccomendedIND', 'PositiveFeedbackCount', 'DivisionName', 'DepartmentName', 'ClassName']
            reviewsDF = pd.read_csv('reviews.csv', names=colnames)

            for review in reviewsDF.ReviewText:
                        review = clean_sents(review)
                        data_words = sent_to_words(review)
                        data_words = [dw for dw in data_words if len(dw)>0]
                        # Build the bigram and trigram models
                        bigram = gensim.models.Phrases(data_words, min_count=5, threshold=100) # higher threshold fewer phrases.
                        trigram = gensim.models.Phrases(bigram[data_words], threshold=100)  
                        
                        # Faster way to get a sentence clubbed as a trigram/bigram
                        bigram_mod = gensim.models.phrases.Phraser(bigram)
                        trigram_mod = gensim.models.phrases.Phraser(trigram)
                        
                        # See trigram example
                        #print(trigram_mod[bigram_mod[data_words[0]]])

                        # Remove Stop Words
                        data_words_nostops = remove_stopwords(data_words)
                        #print(data_words_nostops)
                        
                        # Form Bigrams
                        data_words_bigrams = make_bigrams(data_words_nostops)
                        #print(data_words_bigrams)
                        
                        # In the end, we didn't create trigrams. Should have taken the extra time.
                        
                        # Initialize spacy 'en' model, keeping only tagger component (for efficiency)
                        # python3 -m spacy download en
                        nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])
                        
                        # Do lemmatization keeping only noun, adj, vb, adv
                        data_lemmatized = lemmatization(data_words_bigrams, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV'])
                        #print(data_lemmatized[:1])
            
                        # Create Dictionary
                        id2word = corpora.Dictionary(data_lemmatized)
                        
                        # Create Corpus
                        texts = data_lemmatized
                        
                        # Term Document Frequency
                        corpus = [id2word.doc2bow(text) for text in texts]
                        
                        #print ([[(id2word[id], freq) for id, freq in cp] for cp in corpus])
                        speeches_corpus = dict(id2word)
                        #print(speeches_corpus)

                        num_topics = 10
                        #print(corpus)
                        #print(len(corpus))
                        lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                                                   id2word=id2word,
                                                                   num_topics=num_topics, 
                                                                   random_state=100,
                                                                   update_every=1,
                                                                   chunksize=100,
                                                                   passes=10,
                                                                   alpha='auto',
                                                                   per_word_topics=True)
                        #print(lda_model.print_topics())
                        #doc_lda = lda_model[corpus]
                        #https://stackoverflow.com/questions/40840731/valueerror-cannot-compute-lda-over-an-empty-collection-no-terms
                        #print ([itm for itm in dir(doc_lda) if not itm.startswith('__')])
                        #print ([itm for itm in dir(doc_lda.obj) if (not itm.startswith('__')) and (not itm.startswith('_'))])
                        #doc_lda.obj.print_topics(num_topics=10)
                        #print(id2word)

                                                
                        #vis = pyLDAvis.gensim.prepare(lda_model, corpus, id2word)
                        #vis"""
            
            
                        
