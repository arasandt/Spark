#import random
#import findspark
#findspark.init()
from time import time
from pprint import pprint

def parse_N_calculate_age(data):
             userid,age,gender,occupation,zip = data.split("|")
             return  userid, age_group(int(age)),gender,occupation,zip,int(age)

def  age_group(age):
        if age < 10 :
           return '0-10'
        elif age < 20:
           return '10-20'
        elif age < 30:
           return '20-30'
        elif age < 40:
           return '30-40'
        elif age < 50:
           return '40-50'
        elif age < 60:
           return '50-60'
        elif age < 70:
           return '60-70'
        elif age < 80:
           return '70-80'
        else :
           return '80+'
       
        
#import pyspark
#from pyspark import SparkSession
#textFile = spark.read.text("D:\Arasan\Misc\GitHub\Spark\README.md")
from pyspark.sql import SparkSession
#from pyspark import SparkContext
from pyspark import SparkConf
#from pyspark.sql import SQLContext

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
#spark = SparkSession.builder.appName("example-spark").config("spark.app.name","example-spark").getOrCreate()
#spark = SparkSession.builder.getOrCreate()
#sc = SparkContext(conf=conf).getOrCreate()
#print(SparkConf().getAll())
sc = spark.sparkContext.getOrCreate() # see its lowercase
#sqlContext = SQLContext(sc)
##RDDread = sc.textFile ("D:\\Arasan\\Misc\\GitHub\\Spark\\README.md")
#print(RDDread.first())
#print(RDDread.take(10))
#textFile = spark.read.text("D:\\Arasan\\Misc\\GitHub\\Spark\\README.md")
#print(textFile.count())
#print(textFile.first())
#linesWithSpark = textFile.filter(textFile.value.contains("test"))
#print(linesWithSpark.count())

# =============================================================================
# sc = SparkContext.getOrCreate()
# def inside(p):
#     x, y = random.random(), random.random()
#     return x*x + y*y < 1`
# NUM_SAMPLES = 1000000
# count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
# print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
# sc.stop()
# =============================================================================

#confusedRDD = sc.textFile("README.md",5)
#print(confusedRDD.take(5))
#print(confusedRDD.getNumPartitions())
# =============================================================================
# mappedconfusion = confusedRDD.map(lambda line : line.split(" "))
# print()
# print(mappedconfusion.take(5))
# flatMappedConfusion = confusedRDD.flatMap(lambda line : line.split(" "))
# print()
# print(flatMappedConfusion.take(5))
# 
# =============================================================================
# =============================================================================
# onlyconfusion = confusedRDD.filter(lambda line : ("confus" in line.lower()))
# print(onlyconfusion.count())
# print(onlyconfusion.collect())
# =============================================================================

# =============================================================================
# sampledconfusion = confusedRDD.sample(True,0.5,3) #True implies withReplacement
# print(sampledconfusion.collect())
# 
# =============================================================================


# =============================================================================
# abhay_marks = [("physics",85),("maths",75),("chemistry",95)]
# ankur_marks = [("physics",65),("maths",45),("chemistry",85)]
# abhay = sc.parallelize(abhay_marks)
# ankur = sc.parallelize(ankur_marks)
# print(abhay.union(ankur).collect())
# Subject_wise_marks = abhay.join(ankur)
# print(Subject_wise_marks.collect())
# 
# =============================================================================

# =============================================================================
# Cricket_team = ["sachin","abhay","michael","rahane","david","ross","raj","rahul","hussy","steven","sourav"]
# Toppers = ["rahul","abhay","laxman","bill","steve"]
# cricketRDD = sc.parallelize(Cricket_team)
# toppersRDD = sc.parallelize(Toppers)
# toppercricketers = cricketRDD.intersection(toppersRDD)
# print(toppercricketers.collect())
# =============================================================================

# =============================================================================
# best_story = ["movie1","movie3","movie7","movie5","movie8"]
# best_direction = ["movie11","movie1","movie5","movie10","movie7"]
# best_screenplay = ["movie10","movie4","movie6","movie7","movie3"]
# story_rdd = sc.parallelize(best_story)
# direction_rdd = sc.parallelize(best_direction)
# screen_rdd = sc.parallelize(best_screenplay)
# total_nomination_rdd = story_rdd.union(direction_rdd).union(screen_rdd)
# print(total_nomination_rdd.collect())
# unique_movies_rdd = total_nomination_rdd.distinct()
# print(unique_movies_rdd.collect())
# =============================================================================


# =============================================================================
# userRDD = sc.textFile("D:\\Arasan\\Misc\\GitHub\\Spark\\input\\u.user",10)
# print(userRDD.count())
# data_with_age_bucket = userRDD.map(parse_N_calculate_age)
# #print(data_with_age_bucket.collect())
# RDD_20_30 = data_with_age_bucket.filter(lambda line : '20-30' in line)
# #print(RDD_20_30.collect())
# freq = RDD_20_30.map(lambda line : line[3]).countByValue()
# #print(dict(freq))
# age_wise = RDD_20_30.map (lambda line : line[2]).countByValue()
# #print(dict(age_wise))
# RDD_20_30.unpersist()
# 
# Under_age = sc.accumulator(0)
# Over_age = sc.accumulator(0)
# 
# def outliers(data):
#     global Over_age, Under_age
#     age_grp = data[1]
#     if(age_grp == "70-80"):
#         Over_age +=1
#     if(age_grp == "0-10"):
#         Under_age +=1
#     return data
# 
# df = data_with_age_bucket.map(outliers).collect()
# print(Under_age,Over_age)
# 
# =============================================================================


# =============================================================================
# sum_rdd = sc.parallelize(range(1,500))
# print(sum_rdd.reduce(lambda x,y: x+y))
# =============================================================================

# =============================================================================
# df = spark.read.option('header','true').option('inferSchema','true').csv("D:\\Arasan\\Misc\\GitHub\\Seaborn\\input\\ShareRaceByCity.csv")
# #print(df.columns)
# #df = df.orderBy('Geographic area',ascending = False).limit(10).toPandas()[['City','share_asian']]
# #print(df)
# 
# from pyspark.sql.functions import col
# citypy = df.groupBy('Geographic area').agg({'share_white' : 'mean'}).select(col('Geographic area'), col('avg(share_white)').alias('share_white_mean')).orderBy('Geographic area')
# print(citypy.toPandas())
# =============================================================================
#data_file = "C:\\Users\\128537\\Downloads\\kddcup.data_10_percent_corrected"
import os
data_file = os.path.join(os.getcwd(),'input/u.user') 
print(data_file)
raw_data = sc.textFile(data_file)
print(raw_data.count())

# =============================================================================
# normal_raw_data = raw_data.filter(lambda x: 'normal.' in x)
# t0 = time()
# normal_count = normal_raw_data.count()
# tt = time() - t0
# print("There are {} 'normal' interactions".format(normal_count))
# print("Count completed in {} seconds".format(round(tt,3)))
# 
# 
# csv_data = raw_data.map(lambda x: x.split(","))
# t0 = time()
# head_rows = csv_data.take(5)
# tt = time() - t0
# print("Parse completed in {} seconds".format(round(tt,3)))
# pprint(head_rows[0])
# =============================================================================

# =============================================================================
# def parse_interaction(line):
#     elems = line.split(",")
#     tag = elems[41]
#     return (tag, elems)
#  
# key_csv_data = raw_data.map(parse_interaction)
# head_rows = key_csv_data.take(5)
# pprint(head_rows[0])
# 
# =============================================================================
# =============================================================================
# t0 = time()
# all_raw_data = raw_data.collect()
# tt = time() - t0
# print("Data collected in {} seconds".format(round(tt,3)))
# =============================================================================
