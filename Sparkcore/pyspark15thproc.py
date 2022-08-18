#sorting according to 1st column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

rdd=sc.textFile("D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv")

res=rdd.map(lambda x:x.split(',')).filter(lambda x:"age" not in x).map(lambda x:x[3]).sortBy(lambda x:x[3])

for i in res.collect():
    print(i)