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

            files = [None] * 21
            files[0] = open("poe-stories/A_DESCENT_INTO_THE_MAELSTROM", "r")
            files[1] = open("poe-stories/BERENICE", "r")
            files[2] = open("poe-stories/ELEONORA", "r")
            files[3] = open("poe-stories/LANDORS_COTTAGE", "r")
            files[4] = open("poe-stories/MESMERIC_REVELATION", "r")
            files[5] = open("poe-stories/SILENCE_A_FABLE", "r")
            files[6] = open("poe-stories/THE_ASSIGNATION", "r")
            files[7] = open("poe-stories/THE_BLACK_CAT", "r")
            files[8] = open("poe-stories/THE_CASK_OF_AMONTILLADO", "r")
            files[9] = open("poe-stories/THE_DOMAIN_OF_ARNHEIM", "r")
            files[10] = open("poe-stories/THE_FACTS_IN_THE_CASE_OF_M_VALDEMAR", "r")
            files[11] = open("poe-stories/THE_FALL_OF_THE_HOUSE_OF_USHER", "r")
            files[12] = open("poe-stories/THE_IMP_OF_THE_PERVERSE", "r")
            files[13] = open("poe-stories/THE_ISLAND_OF_THE_FAY", "r")
            files[14] = open("poe-stories/THE_MASQUE_OF_THE_RED_DEATH", "r")
            files[15] = open("poe-stories/THE_PIT_AND_THE_PENDULUM", "r")
            files[16] = open("poe-stories/THE_PREMATURE_BURIAL", "r")
            files[17] = open("poe-stories/THE_PURLOINED_LETTER", "r")
            files[18] = open("poe-stories/THE_THOUSAND_AND_SECOND_TALE_OF_SCHEHERAZADE", "r")
            files[19] = open("poe-stories/VON_KEMPELEN_AND_HIS_DISCOVERY", "r")
            files[20] = open("poe-stories/WILLIAM_WILSON", "r")



            for i in range(21):
                        files[i] = files[i].lower

             print(files[0].readlines())

                        


