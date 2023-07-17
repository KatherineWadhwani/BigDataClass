#!/usr/bin/env python3
import time, datetime, sys
from datetime import datetime, date
import pandas as pd
import os
import re
import math
import numpy as np
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession


import nltk
import nltk.corpus
from nltk.corpus import inaugural
nltk.download('inaugural')
nltk.corpus.inaugural.fileids()[-10:]






