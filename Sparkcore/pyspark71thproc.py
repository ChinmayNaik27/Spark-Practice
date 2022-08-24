#dense-rank
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy('state').orderBy(col('zip').desc())
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
ndf=df.withColumn('drnk',dense_rank().over(win))
ndf.show()