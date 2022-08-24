#calculating percent rnk
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
win=Window.partitionBy('State').orderBy(col('zip').desc())
ndf=df.withColumn("prank",percent_rank().over(win))
ndf.show()