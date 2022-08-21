#using Group by
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\Downloads\\bank-full.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').option('sep',';').load(data)
df.printSchema()
res=df.groupBy(col('marital')).agg(sum(col('age')).alias('sumofbalence'))
res.show()