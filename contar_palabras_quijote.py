#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  7 10:34:46 2022

@author: naomipetreaca
"""
from pyspark import SparkContext

sc = SparkContext()

def contar_palabras(infilename, outfilename):
    words = sc.textFile(infilename).flatMap(lambda x: x.split())
    wordsCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda a,b:a +b)
    with open(outfilename, "w") as f:
        f.write(f"{wordsCounts.collect()}\n")

contar_palabras("quijote.txt", "out_hdfs.txt")