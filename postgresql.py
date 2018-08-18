# =============================================================================
# import psycopg2
# #conn = psycopg2.connect("dbname=suppliers user=postgres password=postgres")
# conn=psycopg2.connect(
#   database="postgres",
#   user="postgres",
#   #host="/tmp/",
#   host="/var/run/postgresql/",
#   password="postgres",
#   #port=5432
#   )
# #print(conn)
# cur = conn.cursor()
# sql = "SELECT * FROM playground"
# cur.execute(sql)
# x = cur.fetchall()
# 
# print(x)
# 
# =============================================================================
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
pgDF = spark.read \
.format("jdbc") \
.option("url", "jdbc:postgresql:postgres") \
.option("dbtable", "public.playground") \
.option("user", "postgres") \
.option("password", "postgres") \
.load() \
.createOrReplaceTempView("temp_playground") #need to load into temp table to be able to run parallel using spark

df = spark.sql("select * from temp_playground")

print(df.head())