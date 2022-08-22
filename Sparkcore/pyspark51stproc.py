from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True,sep=',')
ndf=df.withColumn("Current-Day",current_date()).withColumn("nextMonday",next_day(col('Current-Day'),'Mon'))\
    .withColumn("nextfriday",next_day(col('Current-Day'),'Sun'))
ndf.show()