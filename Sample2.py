import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import count



conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
sqlContext = SQLContext(sc)
fifa_df = spark.read.csv("D:\\Arasan\\Misc\\GitHub\\Spark\\input\\WorldCupPlayers.csv", inferSchema = True, header = True)
#fifa_df.show()
#fifa_df.printSchema()
#print(fifa_df.columns)
#print(fifa_df.count())
#print(len(fifa_df.columns))
#fifa_df.describe('Coach Name').show()
#fifa_df.describe('Position').show()
#fifa_df.select('Player Name','Coach Name').distinct().show()
#print(fifa_df.filter(fifa_df.MatchID=='1096').count())
#fifa_df.filter((fifa_df.Position=='C') & (fifa_df.Event=="G40'")).show()
#fifa_df.orderBy(fifa_df.MatchID).show()
#fifa_df.groupby('Team Initials').count().show()
#fifa_df.registerTempTable('fifa_table')
#sqlContext.sql('select * from fifa_table').show()


