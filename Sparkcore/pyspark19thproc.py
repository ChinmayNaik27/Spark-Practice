from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"

res=spark.read.format("csv").option('header','true').load(data)

res.show()