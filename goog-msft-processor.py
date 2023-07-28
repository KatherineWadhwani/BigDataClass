#!/usr/bin/env python3
import pandas as pd
import os
import math
import string
import numpy as np
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from nltk.corpus.reader.util import StreamBackedCorpusView
import sys
import datetime

if __name__ == "__main__":
#Setup          
            sc = SparkContext(appName="Proj7")
            ssc = StreamingContext(sc, 1)

class Streams:
            topGoog = "empty"
            topMsft = "empty"

            def findHigherGoog(tenDay, fortyDay):
                        global topGoog
                        oldTop = topGoog
                        if (tenDay > fortyDay):
                                    topGoog = "tenDay"
                                    if (oldTop == topGoog):
                                                return "1"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                return "2"
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                return "buy "
                        else:
                                    topGoog = "fortyDay"
                                    if (oldTop == topGoog):
                                                return "3"
                                    if (oldTop != topGoog and oldTop == "empty"):
                                                return "4"
                                    if (oldTop != topGoog and oldTop != "empty"):
                                                return "sell "
                                                
            def findHigherMsft(tenDay, fortyDay):
                        global topMsft
                        oldTop = topMsft   
                        if (tenDay > fortyDay):
                                    topMsft = "tenDay"
                                    if (oldTop == topMsft):
                                                return "5"
                                    if (oldTop != topMsft and oldTop == "empty"):
                                                return "6"
                                    if (oldTop != topMsft and oldTop != "empty"):
                                                return "buy "
                        else:
                                    topMsft = "fortyDay"
                                    if (oldTop == topMsft):
                                                return "7"
                                    if (oldTop != topMsft and oldTop == "empty"):
                                                return "8"
                                    if (oldTop != topMsft and oldTop != "empty"):
                                                return "sell "
            
            #Create stream on port 9999 on localhost  
            text_stream =  ssc.socketTextStream("localhost", 9999)
            
            #Create new streams, splitting them into triples (e.g. preform transformation)
            googPrice = text_stream.map(lambda line : (line.split(" ")[0], float(line.split(" ")[1])))
            msftPrice = text_stream.map(lambda line : (line.split(" ")[0], float(line.split(" ")[2])))


            #Create 10-day and 40-day averages
            goog10Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(10, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))\
                                      .filter(lambda x: x[2] == 10)\
                                      .map(lambda line: (line[0], line[1]/10, line[2]))

            goog40Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(40, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))\
                                      .filter(lambda x: x[2] == 40)\
                                      .map(lambda line: (line[0], line[1]/40, line[2]))

            msft10Day = msftPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(10, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))\
                                      .filter(lambda x: x[2] == 10)\
                                      .map(lambda line: (line[0], line[1]/10, line[2]))
            msft40Day = msftPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(40, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))\
                                      .filter(lambda x: x[2] == 40)\
                                      .map(lambda line: (line[0], line[1]/40, line[2]))



            #Join Streams to Generate Signals
            signalGoog = goog10Day.join(goog40Day)\
                                    .map(lambda x: (x[0], x[1][0],  x[1][1], findHigherGoog(x[1][0], x[1][1])))
                                    .reduce(lambda x: (x[0], x[1][0],  x[1][1], findHigherGoog(x[1][0], x[1][1])))
                                    #.filter(lambda x: (x[3]) != "noAlert")\
                                    #.map(lambda x: (x[0], x[3] + "goog"))

            signalMsft = msft10Day.join(msft40Day)\
                                    .map(lambda x: (x[0], x[1][0],  x[1][1], findHigherMsft(x[1][0], x[1][1])))
                                    #.filter(lambda x: (x[3]) != "noAlert")\
                                    #.map(lambda x: (x[0], x[3] + "msft"))

            

            
            #Print streams
            #goog10Day.pprint()
            #goog40Day.pprint()
                                         
            #msft10Day.pprint()
            #msft40Day.pprint()
                                         
            Streams.signalGoog.pprint()
            Streams.signalMsft.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()
