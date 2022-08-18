#sorting the 1st column in descending order
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

rdd=sc.textFile("D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv")
res=rdd.map(lambda x:x.split(',')).filter(lambda x:"name" not in x).map(lambda x:x[0]).sortBy(lambda x:x[0],ascending=False)

for i in res.collect():
    print(i)