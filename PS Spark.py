import findspark
findspark.init()
from pyspark.sql import SparkSession
#from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
path = "D:\\Arasan\\Misc\\GitHub\\Spark\\input\\NYPD_7_Major_Felony_Incidents.csv"
data=sc.textFile(path)
#print(data.take(5))
header = data.first()
#print(header)
dataWOHeader = data.filter(lambda x: x != header)
#dataWOHeader = dataWOHeader.map(lambda x: x.split(','))
#print(dataWOHeader.first())

import csv
from io import StringIO
from collections import namedtuple

fields = header.replace(' ','_').split(',')
#print(fields)

Crime = namedtuple('Crime',fields,verbose=False)

def parse(row):
    reader = csv.reader(StringIO(row))
    row=next(reader)
    return Crime(*row)

crimes = dataWOHeader.map(parse)
#print(crimes.first().Offense)
#print(crimes.map(lambda x: x.Offense).countByValue())
#print(crimes.map(lambda x: x.Occurrence_Year).countByValue())
crimesfiltered = crimes.filter(lambda x: not (x.Offense == "NA" or x.Occurrence_Year == "")).filter(lambda x: int(x.Occurrence_Year) >= 2006)
#print(crimesfiltered.map(lambda x: x.Occurrence_Year).countByValue())

def extractCoords(location):
    location_lat = float(location[1:location.index(",")])
    location_lon = float(location[location.index(",")+1:-1])
    return (location_lat,location_lon)


#print(crimesfiltered.map(lambda x: extractCoords(x.Location_1)).take(10))

crimesfinal = crimesfiltered.filter(lambda x: x.Offense == 'BURGLARY').map(lambda x: x.Occurrence_Year).countByValue()
print(crimesfinal)
