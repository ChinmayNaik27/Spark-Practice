#getting nulls to last value and another data frame to add nulls at first
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
win=Window.partitionBy(col('state')).orderBy(col('zip').desc())
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/bigdata/DATASETS/us-500.csv")
ndf=df.withColumn("lag",lag(col('zip')).over(win)).na.fill(0)
ndf.show()
ndf1=df.withColumn("lead",lead(col('zip')).over(win)).orderBy(col('lead').asc_nulls_last())
ndf1.show()
ndf2=df.withColumn("lead",lead(col('zip')).over(win)).orderBy(col('lead').asc_nulls_first())
ndf2.show()
#this is sql friendly enviornment
df.createOrReplaceTempView('tab')
ndf3=spark.sql("select *,rank() over(order by zip desc) rnk from tab")
ndf3.show()