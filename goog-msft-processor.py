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


            def reducer(word):
                        print(word)
            
            #Create stream on port 9999 on localhost  
            text_stream =  ssc.socketTextStream("localhost", 9999)
            
            #Create new stream off of previous steram (e.g. preform transformation)
            googPrice = text_stream.map(lambda line : (line.split(" ")[0], float(line.split(" ")[1])))
            msftPrice = text_stream.map(lambda line : (line.split(" ")[0], float(line.split(" ")[2])))



            goog10Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(10, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))

            goog40Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(40, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))

            msft10Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(10, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1], a[2] + b[2]))
            msft40Day = googPrice.map(lambda line: (line[0], line[1], 1))\
                                      .window(40, 1)\
                                      .reduce(lambda a, b: (max(a[0], b[0]), a[1] + b[1]/10, a[2] + b[2]))
                                      .filter(lambda x: x[2] == 10)

        
            #Print stream
            goog10Day.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()



