#fetching only unique records (no duplicates)

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
rdd=sc.textFile("D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv")
res=rdd.map(lambda x:x.split(',')).filter(lambda x: "name" not in x).map(lambda x:(x[2])).distinct()
# res= rdd.filter(lambda x: "age" not in x).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2])))
for i in res.collect():
    print(i)