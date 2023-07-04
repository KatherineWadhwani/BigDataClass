#!/usr/bin/env python3
import time, datetime, sys
import pandas as pd
import os
from pyspark import SparkContext
from pyspark import SparkFiles

spark = SparkSession.builder.config(conf=conf) \
            .appName(application_name) \
            .getOrCreate()


spark.sparkContext.addPyFile(SparkFiles.get("access.log"))
