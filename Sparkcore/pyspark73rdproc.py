#creating 4 group for given data by ntile
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy('state').orderBy(col('zip').desc())
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
ndf=df.withColumn('lead',ntile(4))
ndf.show()