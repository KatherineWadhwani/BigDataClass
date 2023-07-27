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

            
            #Create stream on port 9999 on localhost  
            text_stream =  ssc.socketTextStream("localhost", 9999)
            
            #Create new stream off of previous steram (e.g. preform transformation)
            google = text_stream.flatMap(lambda line: line.split (" "))\
                        .filter(lambda x: '-' in x)

            #goog = google.filter(lambda x: x % 6 != 0)
            
            
            
            #Assignment-specific
           
            
            #Print stream
            google.pprint()
            
            #Run
            ssc.start()
            ssc.awaitTermination()



