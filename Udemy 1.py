import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import count, format_number, mean, corr, year, month, max, min
from pyspark.sql.types import IntegerType



conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
sqlContext = SQLContext(sc)
df = spark.read.csv("D:\\Arasan\\Misc\\GitHub\\Spark\\input\\walmart_stock.csv", inferSchema = True, header = True)
#print(df.head())
#print(df.columns)
#print(df.printSchema())
d_df = df.describe()
#print(d_df.select(d_df['Open']).collect())
d_df.select(format_number(d_df['Open'].cast(IntegerType()),2).alias('Open'),
            format_number(d_df['High'].cast(IntegerType()),2).alias('High'),
            format_number(d_df['Low'].cast(IntegerType()),2).alias('Low'),
            format_number(d_df['Close'].cast(IntegerType()),2).alias('Close'),
            format_number(d_df['Volume'].cast(IntegerType()),0).alias('Volume'),
            format_number(d_df['Adj Close'].cast('int'),2).alias('Adj Close')
            )
            #).show()

newdf = df.withColumn('HV Ratio',df['High']/df['Volume'])
#newdf.show()

#print(df.agg({'High':'max'}).collect()[0][0])
#x = df.agg({'High':'max'}).collect()[0][0]
#df.filter(df['High'] == df.agg({'High':'max'}).collect()[0][0]).select('Date').show()
#print(df.orderBy(df['High'].desc()).head(1)[0][0])
#df.agg({'Close':'mean'}).show()
#df.select(mean(df['Close'])).show()
#df.agg({'Volume':'max'}).show()
#df.agg({'Volume':'min'}).show()
df.select(max(df['Volume']),min(df['Volume'])).show()

#df.filter(df['Close'] < 60).agg({'Close':'count'}).show()
#print(df.filter(df['Close'] < 60).count())

#print(df.filter(df['High'] > 80).count() * 100 / df.count())
#print(df.corr('High','Volume'))
#newdf = df.withColumn('Year',year(df['Date'])).groupBy('Year')
#newdf.agg({'High':'max'}).show()
#newdf = df.withColumn('Month',month(df['Date'])).groupBy('Month')
#newdf.agg({'Close':'mean'}).orderBy('Month').show()


#result = df.select(['Date', 'Open', 'High', 'Low', 'Close']).collect()
#print(type(result))