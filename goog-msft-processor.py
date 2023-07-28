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

            def findHigher(tenDay, fortyDay):
                        if (tenDay > fortyDay):
                                    return "tenDay"

                        else:
                                    return "fortyDay"
                                   
            def message (one, two):
                        if (one == two):
                                    return "noSignal"
                        if(two == "fortyDay"):
                                    return "sell "
                        if(two == "tenDay"):
                                    return "buy "
                                    
            
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
                                      .window(5, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))\
                                      .filter(lambda x: x[2] == 5)\
                                      .map(lambda line: (line[0], line[1]/5, line[2]))

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
                                    .map(lambda x: (x[0], x[1][0],  x[1][1], findHigher(x[1][0], x[1][1]), findHigher(x[1][0], x[1][1])))\
                                    .reduce(lambda a, b: (max(a[0], b[0]), max(a[0], b[0]), max(a[0], b[0]), a[0], b[0]))\
                                    .filter(lambda x: (x[3] != x[4]))
                                    #.map(lambda x: (x[0], x[1]))

            signalMsft = msft10Day.join(msft40Day)\
                                    .map(lambda x: (x[0], x[1][0],  x[1][1], findHigher(x[1][0], x[1][1])))
                                    #.reduce(lambda a, b: (max(a[0], b[0]), message(a[2], b[2])))\
                                    #.filter(lambda x: (x[1]) != "noSignal")\
                                    #.map(lambda x: (x[0], x[1] + "msft"))


            
            #Print streams
            #goog10Day.pprint()
            goog40Day.pprint()
                                         
            #msft10Day.pprint()
            #msft40Day.pprint()
                                         
            signalGoog.pprint()
            #signalMsft.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()
