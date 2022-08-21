#ordering data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\Users\\chinm\\Downloads\\bank-full.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',';').load(data)
res=df.groupBy(col('age')).agg(sum(col('balance')).alias('sum_of_amount')).orderBy(col('sum_of_amount'))
res.show()
res1=df.orderBy(col('age'))
res1.show()
res2=df.orderBy(col('age').desc())
res2.show()
df.describe()