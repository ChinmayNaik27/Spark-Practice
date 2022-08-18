from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

rdd=sc.textFile("D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv")

res=rdd.map(lambda x:x.split(',')).filter(lambda x: "name" not in x).map(lambda x:(x[2],int(x[1]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1])

for i in res.collect():
    print(i)