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
player_attributes = spark.read.format('csv').option('header','true').load('D:\\Arasan\\Misc\\GitHub\\Spark\\input\\player_attributes.csv')

#player_count1 = players.count()
#player_count2 = player_attributes.select('player_api_id').distinct().count()
#print(player_count1)
#print(player_count2)
players = players.drop('id','player_fifa_api_id').dropna()
player_attributes = player_attributes.drop('id','player_fifa_api_id','preferred_foot','attacking_work_rate','defensive_work_rate','crossing',
                                           'jumping','sprint_speed','balance','agression','short_passing','potential').dropna()
#print(players.columns)

year_udf = udf(lambda date:date.split('-')[0])
player_attributes = player_attributes.withColumn("year",year_udf(player_attributes.date))
player_attributes = player_attributes.drop('date')
print(player_attributes.columns)
#player_attributes.show(10)

#striker_details = players.join(strikers,players.player_api_id == strikers.player_api_id)   # JOIN statement duplicate key column
#OR
#striker_details = players.join(strikers,['player_api_id'],'inner')   # JOIN statement unqiue key column

#striker_details = players.join(broadcast(strikers),['player_api_id'],'inner')   # broadcast dataframe itself since its of less size

pa_2016 = player_attributes.filter(col('year') == '2016')
#print(pa_2016.columns)
pa_2016.select('player_api_id','overall_rating').coalesce(1).write.option('header','true').csv("players_overall.csv") 
# coalesce tell to bring data into one partition and write the file. If missed, then it will created # of file equal to number of partitions the data resides.
