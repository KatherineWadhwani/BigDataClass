#!/usr/bin/env python3
import time, datetime, sys
import pandas as pd
import os
from pyspark import SparkContext
from pyspark import SparkFiles

spark = SparkContext.getOrCreate()

spark.sparkContext.addPyFile(SparkFiles.get("access.log"))
