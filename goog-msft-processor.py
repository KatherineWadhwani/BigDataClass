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

            top = "empty"

            def findHigher(tenDay, fortyDay):
                        oldTop = top
                        if(top == "empty"):           
                                    if (tenDay > fortyDay):
                                                top = "tenDay"
                                                if (oldTop == top):
                                                            return "noAlert"
                                                if (oldTop != top and oldTop == empty):
                                                            return "noAlert"
                                                if (oldTop != top and oldTop != empty):
                                                            return "golden cross"
                                    else:
                                                top = "fortyDay"
                                                if (oldTop == top):
                                                            return "noAlert"
                                                if (oldTop != top and oldTop == empty):
                                                            return "noAlert"
                                                if (oldTop != top and oldTop != empty):
                                                            return "death cross"
            
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
            signalGoogle = goog10Day.join(goog40Day)\
                                    .map(lambda x: (x[0], "10-Day Average: " + str(x[1][0]), "40-Day Average: " + str(x[1][1]), findHigher(x[1][0], x[1][1])))\
                                    .filter(lambda x: (x[3]) != "noAlert")

             signalMsft = msft10Day.join(msft40Day)\
                                    .map(lambda x: (x[0], "10-Day Average: " + str(x[1][0]), "40-Day Average: " + str(x[1][1]), findHigher(x[1][0], x[1][1])))\
                                    .filter(lambda x: (x[3]) != "noAlert")

            
            #Print stream
            signalGoogle.pprint()
            signalMsft.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()



