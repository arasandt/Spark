import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
from io import StringIO
import csv
from pprint import pprint

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
trafficpath = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\Dodgers.data"
gamespath = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\Dodgers.events"
traffic=sc.textFile(trafficpath)
#print(traffic.take(10))
games=sc.textFile(gamespath)
#print(games.take(10))

def parseTraffic(row):
    DATE_FMT = '%m/%d/%Y %H:%M'
    row = row.split(",")
    row[0] = datetime.strptime(row[0],DATE_FMT)
    row[1] = int(row[1])
    return (row[0],row[1])
    
trafficParsed = traffic.map(parseTraffic)
#print(trafficParsed.take(10))
dailyTrend = trafficParsed.map(lambda x: (x[0].date(),x[1])).reduceByKey(lambda x, y: x+y)
dailyTrend = dailyTrend.sortBy(lambda x: -x[1])
#print(dailyTrend.take(10))


def parseGames(row):
    DATE_FMT = '%m/%d/%y'
    row = row.split(",")
    row[0] = datetime.strptime(row[0],DATE_FMT).date()
    return (row[0],row[4])

gamesParsed = games.map(parseGames)
#print(gamesParsed.take(10))

dailyTrendCombined = dailyTrend.leftOuterJoin(gamesParsed)
#print(dailyTrendCombined.take(10))

def checkgameday(row):
    if row[1][1] == None:
        return (row[0],row[1][1],'Regular Day',row[1][0])
    else:
        return (row[0],row[1][1],'Game Day',row[1][0])


dailyTrendCombinedByGames = dailyTrendCombined.map(checkgameday).sortBy(lambda x: -x[3])    
#pprint(dailyTrendCombinedByGames.take(10))





x = dailyTrendCombinedByGames.map(lambda x: (x[2],x[3])).combineByKey(lambda value: (value,1),# create combiner function (sum, counter))
                                  lambda acc, value: (acc[0]+value,acc[1]+1),# merge function
                                  lambda acc1, acc2: (acc1[0]+acc2[0],acc1[1]+acc2[1])).mapValues(lambda x:x[0]/x[1] ) # merge combiners
print(x.collect())

























