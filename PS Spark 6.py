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

from pyspark.accumulators import AccumulatorParam

class VAccumulator(AccumulatorParam):
    
    def zero(self, value):
        return [0.0] * len(value)
    
    def addInPlace(self,v1,v2):
        for i in range(len(v1)):
            v1[i] += v2[i]
        return v1

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
vc = sc.accumulator([10.0,20.0,30.0],VAccumulator())        
print(vc.value)
vc += [2,2,2]
print(vc.value)
