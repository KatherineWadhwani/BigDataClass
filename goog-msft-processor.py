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
            
            


            findHigherGoog(2, 4)
            findHigherGoog(5, 1)    
          
