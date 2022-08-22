#using dayofofyear,dayofmonth,monthbetween
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.csv("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv",header=True,inferSchema=True)
ndf=df.withColumn("OD",to_date(col('Order Date'),'d-M-yyyy')).withColumn("dayofYear",dayofyear(col("OD")))\
    .withColumn("dayofmonth",dayofmonth(col("OD"))).withColumn("MonthBetween",months_between(current_date(),col("OD")))
ndf.show(truncate=False)