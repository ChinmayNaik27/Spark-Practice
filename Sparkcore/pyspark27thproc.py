#importing data from a csv file  and adding a new column with defalut value of one
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
#if column deos not exists then update or else add
ndf=df.withColumn('Quantitynew',lit(1))
ndf.show()
ndf.printSchema()
