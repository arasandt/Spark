import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
from io import StringIO
import csv
from pprint import pprint as print

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
bookspath = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\Books.txt"
characterspath = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\Characters.txt"
edgespath = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\Edges.txt"

books=sc.textFile(bookspath)
characters=sc.textFile(characterspath)
edges=sc.textFile(edgespath)


def edgefilter(row):
    if '*' in row or '"' in row:
        return False
    else:
        return True
        
    
edgesfil = edges.filter(edgefilter)

#print(books.take(1))
#print(characters.take(1))
#print(edgesfil.take(1))

characterbookmap = edgesfil.map(lambda x: x.split()).map(lambda x: (x[0],x[1:]))
#print(characterbookmap.take(10))

def charparse(row):
    row = row.split(':')
    return (row[0][7:],row[1].strip())

characterlookup = characters.map(charparse).collectAsMap()
#print(characterlookup)

characterstrength = characterbookmap.mapValues(lambda x: len(x)).map(lambda x: (characterlookup[x[0]],x[1])).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])
#print(characterstrength.take(10))


bookcharactermap = characterbookmap.flatMapValues(lambda x: x).map(lambda x: (x[1],x[0])).reduceByKey(lambda x,y: x+","+y).mapValues(lambda x: x.split(','))
#print(bookcharactermap.take(10))

from itertools import  combinations
coocurrencemap = bookcharactermap.flatMap(lambda x: list(combinations(x[1],2)))
#print(coocurrencemap.take(10))

coocurrencestrength = coocurrencemap.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)\
                                    .map(lambda x: (x[0][0],x[0][1],x[1]))\
                                    .sortBy(lambda x: -x[2])\
                                    .map(lambda x: (characterlookup[x[0]],characterlookup[x[1]],x[2]))
print(coocurrencestrength.take(10))













