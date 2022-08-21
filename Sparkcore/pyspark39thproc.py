#extracting first and last
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
rdd=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)

nrdd=rdd.withColumn("index numbers",substring_index(col('Order Date'),'-',-2))
nrdd.show()