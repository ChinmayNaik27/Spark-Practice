#finding lastday
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("CDay",current_date()).withColumn("od",to_date(col("Order Date"),'d-M-yyyy')).withColumn("last_date",last_day(col('Od')))
ndf.show()