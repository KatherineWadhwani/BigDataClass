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

            def signalGoog(d1, d2):
                        trend1 = "null"
                        trend2 = "null"
                        if (d1[1][0] > d1[1][1]):
                                    trend1 = "tenDay"
                        if (d1[1][0] < d1[1][1]):
                                    trend1 = "fortyDay"
                        if (d2[1][0] > d2[1][1]):
                                    trend2 = "tenDay"
                        if (d2[1][0] < d2[1][1]):
                                    trend2 = "fortyDay"
                        if (trend1 == "tenDay" and trend2 == "fortyDay"):
                                    return str(d2[0]) + " sell goog"
                        if (trend1 == "fortyDay" and trend2 == "tenDay"):
                                    return str(d2[0]) + " buy goog"
                        return "null"

            def signalMsft(d1, d2):
                        trend1 = "null"
                        trend2 = "null"
                        if (d1[1][0] > d1[1][1]):
                                    trend1 = "tenDay"
                        if (d1[1][0] < d1[1][1]):
                                    trend1 = "fortyDay"
                        if (d2[1][0] > d2[1][1]):
                                    trend2 = "tenDay"
                        if (d2[1][0] < d2[1][1]):
                                    trend2 = "fortyDay"
                        if (trend1 == "tenDay" and trend2 == "fortyDay"):
                                    return str(d2[0]) + " sell msft"
                        if (trend1 == "fortyDay" and trend2 == "tenDay"):
                                    return str(d2[0]) + " buy msft"
                        return "null"


           
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
                                    .window(2, 1)\
                                    .reduce(lambda d1, d2: signalGoog(d1, d2))\
                                    .filter(lambda x: "buy" in x or "sell" in x)


            signalMsft = goog10Day.join(goog40Day)\
                                    .window(2, 1)\
                                    .reduce(lambda d1, d2: signalGoog(d1, d2))\
                                    .filter(lambda x: "buy" in x or "sell" in x)


            
            #Print streams
            #goog10Day.pprint()
            #goog40Day.pprint()
                                         
            #msft10Day.pprint()
            #msft40Day.pprint()

            signalGoog.pprint()
            signalMsft.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()
