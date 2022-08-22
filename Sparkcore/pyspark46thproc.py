#createing current date and time stamp
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:\\Users\\chinm\\OneDrive\\Documents\\record1.csv")
ndf=df.withColumn("New1",to_date(col('Order Date'),'d-M-yyyy')).withColumn("currentdate",current_date()).withColumn('current_timestamp',current_timestamp())
#this is according to server date
ndf.show(truncate=False)