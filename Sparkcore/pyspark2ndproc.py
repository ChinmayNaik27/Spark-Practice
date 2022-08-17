from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="D:\\Big-Data-Files_&_notes\\Spark\\drivers\\asl.csv"
sc=spark.sparkContext
ardd=sc.textFile(data)
res=ardd.map(lambda x:x.split(',')).filter(lambda x:'hyd' in x[2])
print(res.collect())
