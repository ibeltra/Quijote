#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: isabelbeltramerino
"""
from pyspark import SparkContext
from random import random
sc = SparkContext()

def porcentaje_archivo(infilename, outfilename, porcentaje):
    infile = sc.textFile(infilename)
    lineas = infile.map(lambda x: x.strip())
    lineas = lineas.filter(lambda x: random()<= porcentaje)
    lineas = lineas.collect()
    with open(outfilename, "w") as outfile: 
        outfile.write(f"{lineas}\n")