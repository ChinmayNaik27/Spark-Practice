#creating a udf function and suing it
from pyspark.sql import *
from pyspark.sql.functions import *

def sparkfunc(str):
    if str=='Kentucky':
        return "20 % Off"
    elif str=='Florida':
        return "35 % Off"
    else:
        return "10 % Off"

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

rdd=spark.read.format('csv').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
uf=udf(sparkfunc)
ndf=rdd.withColumn("Discounts Today",uf(col('state')))
ndf.show()