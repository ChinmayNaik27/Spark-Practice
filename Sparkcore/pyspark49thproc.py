#substracting days
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("Current-date",current_date()).withColumnRenamed('Order Date','orderdate').withColumn("New date",date_sub(col('Current-date'),100))
ndf.show()
ndf1=df.withColumn("Order Date",to_date(col('Order Date'),'d-M-yyyy')).withColumn("Current-date",current_date()).withColumn("New date",date_sub(col('Current-date'),100)).withColumn("type2 output",date_add(col('Current-date'),-100))
ndf1.show(truncate=False)