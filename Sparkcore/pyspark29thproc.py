#replaceing columns
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv"

df=spark.read.format("csv").option('header','true').option('inferSchema','true').load(data)
ndf=df.withColumn("NewCol1",regexp_replace(col('Product ID'),'-',''))
ndf.show()
#replacing existing columns
ndf1=df.withColumn("Product ID",regexp_replace('Product ID',"-",""))
ndf1.show()