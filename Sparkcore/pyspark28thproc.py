#specifing conditions for column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn('country_in_short',when(col('city')=='Henderson','HS'))
ndf.show()
ndf=df.withColumn('state_short1',when(df.State=='Kentucky','KN').otherwise(col('State'))) #to fills data of state whereever value does not match
ndf.show()
