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
            A_DESCENT = open("poe-stories/A_DESCENT_INTO_THE_MAELSTROM", "r")
            print(A_DESCENT.read())

            BERENICE = open("poe-stories/BERENICE", "r")
            print(BERENICE.read())
            
            ELEONORA = open("poe-stories/ELEONORA", "r")
            print(ELEONORA.read())

            LANDORS_COTTAGE = open("poe-stories/LANDORS_COTTAGE", "r")
            print(LANDORS_COTTAGE.read())

            MESMERIC_REVELATION = open("poe-stories/MESMERIC_REVELATION", "r")
            print(MESMERIC_REVELATION.read())
            
            SILENCE_A_FABLE = open("poe-stories/SILENCE_A_FABLE", "r")
            print(SILENCE_A_FABLE.read())

            THE_ASSIGNATION = open("poe-stories/THE_ASSIGNATION", "r")
            print(THE_ASSIGNATION.read())

            THE_BLACK_CAT = open("poe-stories/THE_BLACK_CAT", "r")
            print(THE_BLACK_CAT.read())
            
            THE_CASK_OF_AMONTILLADO = open("poe-stories/THE_CASK_OF_AMONTILLADO", "r")
            print(THE_CASK_OF_AMONTILLADO.read())

            THE_DOMAIN_OF_ARNHEIM = open("poe-stories/THE_DOMAIN_OF_ARNHEIM", "r")
            print(THE_DOMAIN_OF_ARNHEIM.read())

            THE_FACTS_IN_THE_CASE_OF_M_VALDEMAR = open("poe-stories/THE_FACTS_IN_THE_CASE_OF_M._VALDEMAR", "r")
            print(THE_FACTS_IN_THE_CASE_OF_M_VALDEMAR.read())

            THE_FALL_OF_THE_HOUSE_OF_USHER = open("poe-stories/THE_FALL_OF_THE_HOUSE_OF_USHER", "r")
            print(THE_FALL_OF_THE_HOUSE_OF_USHER.read())

            THE_IMP_OF_THE_PERVERSE = open("poe-stories/THE_IMP_OF_THE_PERVERSE", "r")
            print(THE_IMP_OF_THE_PERVERSE.read())

            THE_ISLAND_OF_THE_FAY = open("poe-stories/THE_ISLAND_OF_THE_FAY", "r")
            print(THE_ISLAND_OF_THE_FAY.read())

            THE_MASQUE_OF_THE_RED_DEATH = open("poe-stories/THE_MASQUE_OF_THE_RED_DEATH", "r")
            print(THE_MASQUE_OF_THE_RED_DEATH.read())

            THE_PIT_AND_THE_PENDULUM = open("poe-stories/THE_PIT_AND_THE_PENDULUM", "r")
            print(THE_PIT_AND_THE_PENDULUM.read())

            THE_PREMATURE_BURIAL = open("poe-stories/THE_PREMATURE_BURIAL", "r")
            print(THE_PREMATURE_BURIAL.read())

            THE_PURLOINED_LETTER = open("poe-stories/THE_PURLOINED_LETTER", "r")
            print(THE_PURLOINED_LETTER.read())

            THE_THOUSAND_AND_SECOND_TALE_OF_SCHEHERAZADE = open("poe-stories/THE_THOUSAND_AND_SECOND_TALE_OF_SCHEHERAZADE", "r")
            print(THE_THOUSAND_AND_SECOND_TALE_OF_SCHEHERAZADE.read())

            VON_KEMPELEN_AND_HIS_DISCOVERY = open("poe-stories/VON_KEMPELEN_AND_HIS_DISCOVERY", "r")
            print(VON_KEMPELEN_AND_HIS_DISCOVERY.read())

            WILLIAM_WILSON = open("poe-stories/WILLIAM_WILSON", "r")
            print(WILLIAM_WILSON.read())

