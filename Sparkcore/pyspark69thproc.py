#using window functions
#adding rownumber to columns
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.orderBy(col('first_name'))
df=spark.read.csv("C:/bigdata/DATASETS/us-500.csv",header=True,inferSchema=True)
ndf=df.withColumn('rownum',row_number().over(win))
ndf.show()