#droping columns
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
ndf=df.withColumn("newcol",regexp_replace(col('order id'),"-",'')).drop("country",'city','state')
ndf.show()