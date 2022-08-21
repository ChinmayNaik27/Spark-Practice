#importing data from a csv file  and adding a new column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('csv').option('header','true').option('inferSchama','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
#if column deos not exists then
df.show()