#datetrunc function usage
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
ndf=df.withColumn("OD",to_date(col('Order Date'),'d-M-yyyy')).withColumn("date_truc_year",date_trunc('year',col('OD')))\
    .withColumn("date_trunc_month",date_trunc("month",col("OD"))).withColumn("date_trunc_day",date_trunc("day",col("OD")))
ndf.show()