#reading unstructured data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\Downloads\\GMT20220815-054205_Recording.txt"
rdd=sc.textFile(data)
res=rdd.map(lambda x:x.split('\t')).map(lambda x:(x[0],x[1]))

for i in res.collect():
    print(i)