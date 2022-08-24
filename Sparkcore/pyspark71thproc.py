#dense-rank
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy('state').orderBy(col('city').desc())
df=