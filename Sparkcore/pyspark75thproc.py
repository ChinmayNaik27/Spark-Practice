#calculating difference between lag and lead
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
win=Window.partitionBy('state').orderBy(col('zip').desc())
ndf=df.withColumn("lag",lag(col('zip')).over(win)).withColumn("lead",lead(col('zip')).over(win)).withColumn("diff",(col('lead')-col('lag')))
ndf.show()