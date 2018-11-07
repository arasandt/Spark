import findspark
findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row
from pyspark.sql.functions import lit, col, udf, broadcast
import pyspark.sql.functions as F
from pyspark import SparkConf
from datetime import datetime
from io import StringIO
import csv
#from pprint import pprint as print

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
players = spark.read.format('csv').option('header','true').load('D:\\Arasan\\Misc\\GitHub\\Spark\\input\\player.csv')