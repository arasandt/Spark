import findspark
findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import Row
from pyspark.sql.functions import lit, col
from pyspark import SparkConf
from datetime import datetime
from io import StringIO
import csv
from pprint import pprint as print

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase

simple_data = sc.parallelize([1,'Alice',50])
# =============================================================================
# print(simple_data.count())
# print(simple_data.first())
# print(simple_data.take(2))
# print(simple_data.collect())
# 
# =============================================================================

#simple_data = sc.parallelize([Row(id=1,name='Alice',score=50)])
#df = simple_data.toDF()
#df.show()
sqlcontext = SQLContext(sc)
df = sqlcontext.range(5)
#df.show()
data =  [('Alice',50),('Bob',40)]
df = sqlcontext.createDataFrame(data,['Name','Score'])
#df.show()

simple_data = sc.parallelize([Row(1,'Alice',50),Row(2,'Bob',40)])
column_names = Row('id','name','score')
students = simple_data.map(lambda x : column_names(*x))
#print(students.collect())
students_df = sqlcontext.createDataFrame(students)
#students_df.show()
rdd_students = students_df.rdd.map(lambda x: (x.id)).collect()
#print(rdd_students)

#print(students_df.select('id','name').withColumn('IDS',col('id') + lit(1)).collect())
#print(students_df.select(col('id').alias('idsd'),'name').withColumn('IDS',col('idsd') + lit(1)).withColumnRenamed("IDS",'IIDS').collect())


students_df_p = students_df.toPandas()
print(students_df_p)



