#changing column values to mask having discount wherever discount has .
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("NewCOl",when(col('Discount').contains('.'),'*************').otherwise(col('Discount')))
ndf.show()
