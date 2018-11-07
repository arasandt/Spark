import findspark
findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row
from pyspark.sql.functions import lit, col
import pyspark.sql.functions as F
from pyspark import SparkConf
from datetime import datetime
from io import StringIO
import csv
from pprint import pprint as print

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
data = spark.read.format('csv').option('header','true').load('D:\\Arasan\\Misc\\GitHub\\Spark\\input\\london_crime_by_lsoa.csv')
#data.printSchema()
#print(data.count())
#data.limit(10).show()
data.dropna()
data = data.drop('lsoa_code')
total_boroughs = data.select('borough').distinct()
#total_boroughs.show()
hackney_data = data.filter(data['borough'] == 'Hackney')
#hackney_data.show()

data_2015_2016 = data.filter(data['year'].isin(['2015','2016']))
#data_2015_2016.sample(fraction=0.1).show()
borough_crime_count = data.groupBy('borough').count()
#borough_crime_count.show()
borough_conviction_sum = data.groupBy('borough').agg({'value':'sum'}).withColumnRenamed('sum(value)','convictions')
#borough_conviction_sum.show()
borough_conviction_total = borough_conviction_sum.agg({'convictions':'sum'})
total_convictions = borough_conviction_total.collect()[0][0]
borough_percentage = borough_conviction_sum.withColumn('% convictions',F.round(borough_conviction_sum.convictions * 100 / total_convictions, 2))
borough_percentage = borough_percentage.orderBy(borough_percentage[2].desc())
#borough_percentage.show()
convictions_monthly = data.filter(data['year'] == '2016').groupBy('month').agg({'value':'sum'}).withColumnRenamed('sum(value)','convictions')
#convictions_monthly.show()

crimes_category = data.groupBy('major_category').agg({'value':'sum'}).withColumnRenamed('sum(value)','convictions')
#crimes_category = data.groupBy('major_category').agg({'value':'count'}).withColumnRenamed('count(value)','convictions')
#crimes_category.show()
year_df = data.select('year')
#year_df.describe().show()
data_ct = data.crosstab('borough','major_category')
data_ct.show()
