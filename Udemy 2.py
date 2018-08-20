import findspark
findspark.init()
from pyspark.sql import SparkSession
#from pyspark import SparkContext
from pyspark import SparkConf
#from pyspark.sql import SQLContext
#from pyspark.sql.functions import count, format_number, mean, corr, year, month, max, min
#from pyspark.sql.types import IntegerType



conf = SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','4g')])
spark = SparkSession.builder.appName("example-spark").config(conf=conf).getOrCreate()
sc = spark.sparkContext.getOrCreate() # see its lowercase
#sqlContext = SQLContext(sc)
#df = spark.read.csv("D:\\Arasan\\Misc\\GitHub\\Spark\\input\\walmart_stock.csv", inferSchema = True, header = True)
from pyspark.ml.regression import LinearRegression
training = spark.read.format('libsvm').load('D:\\Arasan\\Misc\\GitHub\\Spark\\input\\sample_linear_regression_data.txt')
#training.show()
lr = LinearRegression(featuresCol='features',labelCol='label',predictionCol='prediction')
lrmodel = lr.fit(training)
#print(lrmodel.coefficients) #they are basically the weights assigned to variable.
#print(lrmodel.intercept) # when all variable are zero, what is the result. i.e, answer for no input.
training_summary = lrmodel.summary
print(training_summary.r2) # expected / actual. Should be high for a best fit model (generally) between 0 and 1
print(training_summary.rootMeanSquaredError) # how much our predictions deviate, on average, from the actual values in the dataset. Still measures fit.
all_data = spark.read.format('libsvm').load('D:\\Arasan\\Misc\\GitHub\\Spark\\input\\sample_linear_regression_data.txt')
split_object = all_data.randomSplit([0.7,0.3])