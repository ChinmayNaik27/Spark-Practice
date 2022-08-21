#fetch unique records
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
ndf1=df.groupBy(col('City')).agg(count(col('*')).alias('new col'),collect_set('Ship Mode').alias('Ship Mode'))
ndf1.show(truncate=False)