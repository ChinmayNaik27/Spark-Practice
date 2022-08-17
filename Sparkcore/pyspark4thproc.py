#filtering integer data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"
rdd=sc.textFile(data)
res=rdd.map(lambda x:x.split(',')).filter(lambda x:"age" not in x).filter(lambda x:int(x[1])>=30)
for i in res.collect():
    print(i)