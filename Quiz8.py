#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import math
import string
import re
import pprint
import nltk
import csv
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
import glob


#Setup 

all_files = glob.glob(os.path.join("ml-latest-small/", "*.csv"))

df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
print(df.keys())
