from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master('local[*]').appName("chinmay").getOrCreate()

data=[1,2,3,4,5]
sc=spark.sparkContext
s1=sc.parallelize(data)
print(s1.collect())