#using count *
from pyspark.sql import *
from pyspark.sql.functions import *

import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\Users\\chinm\\Downloads\\bank-full.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',';').load(data)

res=df.groupBy(col('age')).agg(count(col('*')).alias('count_total'))
res.show()